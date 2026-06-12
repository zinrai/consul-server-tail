# consul-server-tail

A read-only, near-real-time terminal view of a Consul server cluster, for watching cluster state during a rolling server migration.

## What it does

Polls the HTTP API of each Consul server every few seconds and prints, in one screen, four cluster-level checks (leader agreement, peer composition, node-id uniqueness, index convergence) and a one-line summary per node, with the node name next to its address. Names come from each node's own answer (`Config.NodeName` in `/v1/agent/self`); a node that cannot answer shows `-`. Any node that disagrees with the majority is expanded in place to its full raw `raft list-peers` and `members` output, so detail appears only where there is a problem. A node that cannot be reached is shown as a single UNREACHABLE line. The dropped/rejoined transition log keeps node names, recorded at the moment of the event from the cycle in which the node last answered.

It does not change cluster state. It issues only GET requests and runs no `consul` write commands.

The four checks mirror what HashiCorp's upgrade process tells you to confirm by hand: a single leader with enough voters, and `commit_index` / `last_log_index` in sync across servers. See the link under "A note on --index-tolerance" below.

## Requirements

- Python 3 (standard library only; no third-party packages)
- Each target server's HTTP API reachable over plain HTTP, without an ACL token

## Usage

```
./consul-server-tail.py --nodes 10.0.0.1:8500 10.0.0.2:8500 10.0.0.3:8500
```

Options:

```
--nodes            space-separated Consul HTTP addresses (required)
--expect           expected number of voting peers (default 5)
--interval         polling interval in seconds (default 5)
--timeout          per-request HTTP timeout in seconds (default 2.0)
--index-tolerance  max acceptable inter-node index gap (default 100)
```

Example output for a healthy five-node cluster:

```
consul-server-tail  17:32:30  interval=5s expect=5
--------------------------------------------------------------------------------------
[OK ] leader-agreement all agree: 10.0.0.3:8300
[OK ] peer-composition leader=1 voters=5
[OK ] node-id-unique   5 unique ids
[OK ] index-converge   commit_gap=0 last_log_gap=0 (tol=100)
--------------------------------------------------------------------------------------
10.0.0.1         consul-server01  follower leader=10.0.0.3:8300          commit=323858050 last_log=323858050  ok
10.0.0.2         consul-server02  follower leader=10.0.0.3:8300          commit=323858050 last_log=323858050  ok
10.0.0.3         consul-server03  leader   leader=10.0.0.3:8300          commit=323858050 last_log=323858050  ok
10.0.0.4         consul-server04  follower leader=10.0.0.3:8300          commit=323858050 last_log=323858050  ok
10.0.0.5         consul-server05  follower leader=10.0.0.3:8300          commit=323858050 last_log=323858050  ok
--------------------------------------------------------------------------------------
recent transitions:
  (none yet)
```

When a node loses its view of the leader, its check lines turn to `BAD` and that node is expanded:

```
[BAD] leader-agreement (none)<-10.0.0.1 | 10.0.0.3:8300<-10.0.0.2,10.0.0.3,10.0.0.4,10.0.0.5
[BAD] peer-composition config unavailable on: 10.0.0.1:No cluster leader
...
NODE 10.0.0.1 consul-server01  (believes leader: -)  <- differs from majority
  index: commit=323858050 last_log=323858050
  raft list-peers: ERROR No cluster leader
  members (servers):
    ...
```

Press Ctrl-C to stop.

### A note on --index-tolerance

`commit_index` and `last_log_index` are the fields HashiCorp's upgrade process uses to confirm a restarted server has rejoined the cluster and caught up with the leader: after restarting a server you check that the two fields hold the same value, which avoids an unexpected leadership election from loss of quorum. This tool watches the same two fields across all servers continuously. See the General upgrade process:

  https://developer.hashicorp.com/consul/docs/upgrade/instructions/general

The index-converge check compares `commit_index` across nodes and `last_log_index` across nodes as two separate gaps, and flags the cluster when either gap exceeds the tolerance. Consul defines no fixed "indexes must be within N" rule; the tolerance is an empirical margin that depends on the cluster's write rate and the polling interval. On an idle cluster the gap stays near zero. A busy cluster can show a larger gap while remaining healthy, so raise the value if needed.

## License

This project is licensed under the [MIT License](LICENSE).
