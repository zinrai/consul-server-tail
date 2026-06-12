#!/usr/bin/env python3

# consul-server-tail: a read-only, near-real-time terminal view of a Consul
# server cluster during a rolling server migration. What is displayed and
# how to read it: README.md.
#
# Maintainer notes: the unit of observation is the node, not the cluster --
# every check compares per-node views, which no single node can answer.
# Plain polling, not blocking queries: the tool runs exactly while nodes
# are expected to drop and rejoin. Standard library only. Written in a
# deliberately plain dialect (no comprehensions, no ternaries, no lambda;
# nesting kept to for -> if) so the code can be scanned at a steady pace.

import argparse
import json
import sys
import time
import urllib.error
import urllib.request

RULE = "-" * 86
MEMBER_STATUS = {0: "none", 1: "alive", 2: "leaving", 3: "left", 4: "failed"}


def get_json(addr, path, timeout):
    # urlopen raises urllib.error.HTTPError for non-2xx responses, so there
    # is no status code to check here. Callers that care about the error
    # body (e.g. raft configuration's 500 "No cluster leader") catch
    # HTTPError and read it from the exception.
    with urllib.request.urlopen("http://" + addr + path, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def parse_uint(s):
    try:
        return int(s)
    except (TypeError, ValueError):
        return None


def short_addr(a):
    if ":" in a:
        return a.rsplit(":", 1)[0]
    return a


def short_id(i):
    return i[:8]


def or_dash(text):
    if text:
        return text
    return "-"


def has_indexes(st):
    return st["commit_index"] is not None and st["last_log_index"] is not None


# --- polling ---
#
# One fetch function per endpoint; each owns exactly one failure policy,
# stated in its docstring. Missing pieces stay falsy so the checks can
# treat an agent as partially-observed without special cases.


def poll_one(addr, timeout):
    st = {
        "addr": addr,
        "reachable": False,
        "err": "",
        "leader_view": "",
        "node_name": "",
        "commit_index": None,
        "last_log_index": None,
        "peers": None,
        "peers_err": "",
        "members": None,
    }
    if not fetch_leader_view(st, timeout):
        return st
    fetch_agent_self(st, timeout)
    fetch_raft_config(st, timeout)
    fetch_members(st, timeout)
    return st


def fetch_leader_view(st, timeout):
    """Fatal on failure: an agent that cannot answer /v1/status/leader is
    recorded as unreachable, with the reason in err, and no further
    endpoints are queried. Returns whether the agent is reachable."""
    try:
        leader = get_json(st["addr"], "/v1/status/leader", timeout)
    except (urllib.error.URLError, OSError, ValueError) as e:
        st["err"] = str(e)
        return False
    st["reachable"] = True
    if isinstance(leader, str):
        st["leader_view"] = leader
    return True


def fetch_agent_self(st, timeout):
    """Silent on failure: indexes stay None, node_name stays empty, and the
    affected displays show "no index data" and "-". The failure reason is
    not worth a line on screen. The node name is the node's own report;
    there is no other source, so an unreachable node displays "-"."""
    try:
        info = get_json(st["addr"], "/v1/agent/self", timeout)
    except (urllib.error.URLError, OSError, ValueError):
        return
    st["node_name"] = info.get("Config", {}).get("NodeName", "")
    raft = info.get("Stats", {}).get("raft", {})
    st["commit_index"] = parse_uint(raft.get("commit_index"))
    st["last_log_index"] = parse_uint(raft.get("last_log_index"))


def peer_record(srv):
    """One server row of /v1/operator/raft/configuration, in the shape the
    checks and the renderer consume."""
    if srv.get("Leader"):
        state = "leader"
    else:
        state = "follower"
    return {
        "node": srv.get("Node", ""),
        "id": srv.get("ID", ""),
        "address": srv.get("Address", ""),
        "state": state,
        "voter": bool(srv.get("Voter")),
    }


def fetch_raft_config(st, timeout):
    """Failure is captured, not hidden: raft configuration returns HTTP 500
    with body "No cluster leader" when raft has no leader -- a legitimate
    state we want to show. The error body goes into peers_err and the
    peer-composition check displays it."""
    try:
        cfg = get_json(st["addr"], "/v1/operator/raft/configuration", timeout)
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", "replace").strip()[:60]
        if not body:
            body = "HTTP %d" % e.code
        st["peers_err"] = body
        return
    except (urllib.error.URLError, OSError, ValueError):
        st["peers_err"] = "unavailable"
        return
    peers = []
    for srv in cfg.get("Servers", []):
        peers.append(peer_record(srv))
    st["peers"] = peers


def member_record(m):
    """One server row of /v1/agent/members, in the shape the checks and the
    renderer consume."""
    return {
        "node": m.get("Name", ""),
        "address": "%s:%s" % (m.get("Addr", ""), m.get("Port", "")),
        "status": MEMBER_STATUS.get(m.get("Status"), str(m.get("Status"))),
        "id": m.get("Tags", {}).get("id", ""),
    }


def fetch_members(st, timeout):
    """Silent on failure: members stay None and the node-id-unique check
    reports the node as having no member data."""
    try:
        raw = get_json(st["addr"], "/v1/agent/members", timeout)
    except (urllib.error.URLError, OSError, ValueError):
        return
    members = []
    for m in raw:
        if m.get("Tags", {}).get("role") == "consul":
            members.append(member_record(m))
    st["members"] = members


# --- checks ---


def evaluate(states, expect, index_tol):
    return [
        check_leader(states),
        check_peers(states, expect),
        check_ids(states),
        check_index(states, index_tol),
    ]


def check_leader(states):
    views = {}
    for s in states:
        if not s["reachable"]:
            continue
        view = s["leader_view"]
        if not view:
            view = "(none)"
        views.setdefault(view, []).append(short_addr(s["addr"]))
    if not views:
        return ("leader-agreement", "??", "no reachable agents")
    if len(views) == 1 and "(none)" not in views:
        only_view = list(views)[0]
        return ("leader-agreement", "OK", "all agree: " + only_view)
    parts = []
    for view, watchers in sorted(views.items()):
        parts.append("%s<-%s" % (view, ",".join(watchers)))
    return ("leader-agreement", "BAD", " | ".join(parts))


def peer_counts(peers):
    """Voter and leader counts in one node's view of the configuration."""
    voters = 0
    leaders = 0
    for p in peers:
        if p["voter"]:
            voters += 1
        if p["state"] == "leader":
            leaders += 1
    return voters, leaders


def check_peers(states, expect):
    failing = []
    voters = 0
    leaders = 0
    saw = False
    for s in states:
        if not s["reachable"]:
            continue
        if s["peers"] is None:
            failing.append("%s:%s" % (short_addr(s["addr"]), s["peers_err"]))
            continue
        saw = True
        node_voters, node_leaders = peer_counts(s["peers"])
        voters = max(voters, node_voters)
        leaders = max(leaders, node_leaders)
    if failing:
        return (
            "peer-composition",
            "BAD",
            "config unavailable on: " + " ".join(failing),
        )
    if not saw:
        return ("peer-composition", "??", "no configuration returned")
    if leaders == 1 and voters == expect:
        return ("peer-composition", "OK", "leader=1 voters=%d" % voters)
    return (
        "peer-composition",
        "...",
        "leader=%d voters=%d (expect leader=1 voters=%d)" % (leaders, voters, expect),
    )


def record_member_ids(members, id_names):
    """Add one node's member rows to the node-id -> node-names index."""
    for m in members:
        if m["id"]:
            id_names.setdefault(m["id"], set()).add(m["node"])


def check_ids(states):
    id_names = {}
    for s in states:
        if not s["members"]:
            continue
        record_member_ids(s["members"], id_names)
    if not id_names:
        return ("node-id-unique", "??", "no member data")
    dups = []
    for node_id, holders in id_names.items():
        if len(holders) > 1:
            dups.append(
                "%s shared by %s" % (short_id(node_id), "+".join(sorted(holders)))
            )
    if dups:
        return ("node-id-unique", "BAD", " | ".join(sorted(dups)))
    return ("node-id-unique", "OK", "%d unique ids" % len(id_names))


def collect_indexes(states, field):
    """The non-missing values of one index field across all nodes."""
    values = []
    for s in states:
        if s[field] is not None:
            values.append(s[field])
    return values


def check_index(states, tol):
    # Two separate gaps: a busy leader's last_log legitimately runs ahead
    # of its commit, so mixing them would inflate the spread. Tolerance
    # semantics and tuning: README, "A note on --index-tolerance".
    commits = collect_indexes(states, "commit_index")
    logs = collect_indexes(states, "last_log_index")
    if not commits or not logs:
        return ("index-converge", "??", "no index data")
    commit_gap = max(commits) - min(commits)
    log_gap = max(logs) - min(logs)
    detail = "commit_gap=%d last_log_gap=%d (tol=%d)" % (commit_gap, log_gap, tol)
    if max(commit_gap, log_gap) <= tol:
        return ("index-converge", "OK", detail)
    return ("index-converge", "...", detail)


# --- divergence detection ---


def peer_sig(s):
    if s["peers"] is None:
        return "ERR:" + s["peers_err"]
    rows = []
    for p in s["peers"]:
        rows.append("%s|%s|%s|%s" % (p["node"], p["id"], p["state"], p["voter"]))
    return ";".join(sorted(rows))


def member_sig(s):
    if s["members"] is None:
        return "NA"
    rows = []
    for m in s["members"]:
        rows.append("%s|%s|%s" % (m["node"], m["status"], m["id"]))
    return ";".join(sorted(rows))


def odd_nodes(states, sig_fn):
    counts = {}
    for s in states:
        if not s["reachable"]:
            continue
        sig = sig_fn(s)
        counts[sig] = counts.get(sig, 0) + 1
    if len(counts) <= 1:
        return set()
    majority = None
    for sig in counts:
        if majority is None or counts[sig] > counts[majority]:
            majority = sig
    odd = set()
    for s in states:
        if s["reachable"] and sig_fn(s) != majority:
            odd.add(short_addr(s["addr"]))
    return odd


# --- rendering ---


def by_node(rec):
    return rec["node"]


def render(states, checks, transitions, interval, expect, tol):
    out = [
        "\x1b[2J\x1b[H",
        "consul-server-tail  %s  interval=%ds expect=%d"
        % (time.strftime("%H:%M:%S"), interval, expect),
        RULE,
    ]
    for name, level, detail in checks:
        out.append("[%-3s] %-16s %s" % (level, name, detail))
    out.append(RULE)

    peer_odd = odd_nodes(states, peer_sig)
    member_odd = odd_nodes(states, member_sig)
    for s in states:
        name = short_addr(s["addr"])
        if not s["reachable"]:
            out.append(
                "%-16s %-16s UNREACHABLE  %s"
                % (name, or_dash(s["node_name"]), s["err"])
            )
        elif name in peer_odd or name in member_odd:
            render_full(out, s)
        else:
            out.append(summary_line(s, name))

    out.append(RULE)
    out.append("recent transitions:")
    if transitions:
        for t in transitions:
            out.append("  " + t)
    else:
        out.append("  (none yet)")
    sys.stdout.write("\n".join(out) + "\n")
    sys.stdout.flush()


def role_of(peers, name):
    """The raft state of the node whose peer address starts with name, as
    seen in one peers list. Unknown when the list is missing or no peer
    matches."""
    if peers is None:
        return "?"
    for p in peers:
        if p["address"].startswith(name + ":"):
            return p["state"]
    return "?"


def summary_line(s, name):
    role = role_of(s["peers"], name)
    if has_indexes(s):
        idx = "commit=%d last_log=%d" % (s["commit_index"], s["last_log_index"])
    else:
        idx = "index=(unavailable)"
    if s["peers"] is None:
        flag = "  <- peers!"
    else:
        flag = "  ok"
    return "%-16s %-16s %-8s leader=%-22s %s%s" % (
        name,
        or_dash(s["node_name"]),
        role,
        or_dash(s["leader_view"]),
        idx,
        flag,
    )


def render_full(out, s):
    name = short_addr(s["addr"])
    out.append(
        "NODE %s %s  (believes leader: %s)  <- differs from majority"
        % (name, or_dash(s["node_name"]), or_dash(s["leader_view"]))
    )
    if has_indexes(s):
        out.append(
            "  index: commit=%d last_log=%d" % (s["commit_index"], s["last_log_index"])
        )
    if s["peers"] is None:
        out.append("  raft list-peers: ERROR %s" % or_dash(s["peers_err"]))
    else:
        out.append("  raft list-peers:")
        out.append(
            "    %-32s %-38s %-22s %-9s %s"
            % ("Node", "ID", "Address", "State", "Voter")
        )
        for p in sorted(s["peers"], key=by_node):
            out.append(
                "    %-32s %-38s %-22s %-9s %s"
                % (
                    p["node"],
                    p["id"],
                    p["address"],
                    p["state"],
                    str(p["voter"]).lower(),
                )
            )
    if s["members"] is not None:
        out.append("  members (servers):")
        out.append("    %-32s %-22s %-8s %s" % ("Node", "Address", "Status", "ID"))
        for m in sorted(s["members"], key=by_node):
            out.append(
                "    %-32s %-22s %-8s %s"
                % (m["node"], m["address"], m["status"], m["id"])
            )
    out.append(RULE)


# --- transition log ---


def majority_leader(states):
    counts = {}
    for s in states:
        if not s["reachable"] or not s["leader_view"]:
            continue
        view = s["leader_view"]
        counts[view] = counts.get(view, 0) + 1
    best = ""
    for view in counts:
        if best == "" or counts[view] > counts[best]:
            best = view
    return best


def record_transitions(prev, cur, log, limit):
    # A dropped node cannot report its name now, but it did one cycle ago:
    # the name comes from prev. A rejoined node reports it again: cur.
    # The leader line stays address-only, because naming a raft address
    # would need a second name source.
    if prev is None:
        return
    prev_by_host = {}
    for s in prev:
        prev_by_host[short_addr(s["addr"])] = s
    now = time.strftime("%H:%M:%S")
    for s in cur:
        name = short_addr(s["addr"])
        if name not in prev_by_host:
            continue
        up = s["reachable"]
        if up == prev_by_host[name]["reachable"]:
            continue
        if up:
            word = "rejoined"
            node_name = s["node_name"]
        else:
            word = "dropped"
            node_name = prev_by_host[name]["node_name"]
        log.append("%s %s %s %s" % (now, name, or_dash(node_name), word))
    prev_leader = majority_leader(prev)
    cur_leader = majority_leader(cur)
    if cur_leader and prev_leader != cur_leader:
        log.append("%s leader is now %s" % (now, cur_leader))
    while len(log) > limit:
        del log[0]


def node_address(s):
    """argparse type hook for --nodes: one host:port token. Rejects commas
    so the old comma-separated syntax fails at parse time with a clear
    message instead of surfacing later as a URL error."""
    if not s or "," in s:
        raise argparse.ArgumentTypeError(
            "addresses are space-separated, not comma-separated: %r" % s
        )
    return s


def main():
    ap = argparse.ArgumentParser(
        description="Near-real-time Consul server cluster view."
    )
    ap.add_argument(
        "--nodes",
        nargs="+",
        type=node_address,
        required=True,
        metavar="HOST:PORT",
        help="Consul HTTP addresses, e.g. --nodes 10.0.0.1:8500 10.0.0.2:8500",
    )
    ap.add_argument(
        "--expect", type=int, default=5, help="expected number of voting peers"
    )
    ap.add_argument(
        "--interval", type=int, default=5, help="polling interval in seconds"
    )
    ap.add_argument(
        "--timeout", type=float, default=2.0, help="per-request HTTP timeout"
    )
    ap.add_argument(
        "--index-tolerance",
        type=int,
        default=100,
        help="max acceptable inter-node index gap; tune per cluster (default 100)",
    )
    args = ap.parse_args()

    transitions = []
    prev = None
    try:
        while True:
            states = []
            for addr in args.nodes:
                states.append(poll_one(addr, args.timeout))
            record_transitions(prev, states, transitions, 12)
            render(
                states,
                evaluate(states, args.expect, args.index_tolerance),
                transitions,
                args.interval,
                args.expect,
                args.index_tolerance,
            )
            prev = states
            time.sleep(args.interval)
    except KeyboardInterrupt:
        sys.stdout.write("\n")


if __name__ == "__main__":
    main()
