In Search of an Understandable
Consensus Algorithm
Diego Ongaro and John Ousterhout, Stanford University
https://www.usenix.org/conference/atc14/technical-sessions/presentation/ongaro

This paper is included in the Proceedings of USENIX ATC ’14:
2014 USENIX Annual Technical Conference.
June 19–20, 2014 • Philadelphia, PA
978-1-931971-10-2

Open access to the Proceedings of
USENIX ATC ’14: 2014 USENIX Annual Technical
Conference is sponsored by USENIX.

In Search of an Understandable Consensus Algorithm
Diego Ongaro and John Ousterhout
Stanford University
Abstract
Raft is a consensus algorithm for managing a replicated
log. It produces a result equivalent to (multi-)Paxos, and
it is as efficient as Paxos, but its structure is different
from Paxos; this makes Raft more understandable than
Paxos and also provides a better foundation for building practical systems. In order to enhance understandability, Raft separates the key elements of consensus, such as
leader election, log replication, and safety, and it enforces
a stronger degree of coherency to reduce the number of
states that must be considered. Results from a user study
demonstrate that Raft is easier for students to learn than
Paxos. Raft also includes a new mechanism for changing
the cluster membership, which uses overlapping majorities to guarantee safety.

1 Introduction
Consensus algorithms allow a collection of machines
to work as a coherent group that can survive the failures of some of its members. Because of this, they play a
key role in building reliable large-scale software systems.
Paxos [13, 14] has dominated the discussion of consensus algorithms over the last decade: most implementations
of consensus are based on Paxos or influenced by it, and
Paxos has become the primary vehicle used to teach students about consensus.
Unfortunately, Paxos is quite difficult to understand, in
spite of numerous attempts to make it more approachable.
Furthermore, its architecture requires complex changes
to support practical systems. As a result, both system
builders and students struggle with Paxos.
After struggling with Paxos ourselves, we set out to
find a new consensus algorithm that could provide a better foundation for system building and education. Our approach was unusual in that our primary goal was understandability: could we define a consensus algorithm for
practical systems and describe it in a way that is significantly easier to learn than Paxos? Furthermore, we wanted
the algorithm to facilitate the development of intuitions
that are essential for system builders. It was important not
just for the algorithm to work, but for it to be obvious why
it works.
The result of this work is a consensus algorithm called
Raft. In designing Raft we applied specific techniques to
improve understandability, including decomposition (Raft
separates leader election, log replication, and safety) and
state space reduction (relative to Paxos, Raft reduces the
degree of nondeterminism and the ways servers can be inconsistent with each other). A user study with 43 students
at two universities shows that Raft is significantly easier

USENIX Association

to understand than Paxos: after learning both algorithms,
33 of these students were able to answer questions about
Raft better than questions about Paxos.
Raft is similar in many ways to existing consensus algorithms (most notably, Oki and Liskov’s Viewstamped
Replication [27, 20]), but it has several novel features:
• Strong leader: Raft uses a stronger form of leadership
than other consensus algorithms. For example, log entries only flow from the leader to other servers. This
simplifies the management of the replicated log and
makes Raft easier to understand.
• Leader election: Raft uses randomized timers to elect
leaders. This adds only a small amount of mechanism
to the heartbeats already required for any consensus algorithm, while resolving conflicts simply and rapidly.
• Membership changes: Raft’s mechanism for changing
the set of servers in the cluster uses a new joint consensus approach where the majorities of two different configurations overlap during transitions. This allows the
cluster to continue operating normally during configuration changes.
We believe that Raft is superior to Paxos and other consensus algorithms, both for educational purposes and as a
foundation for implementation. It is simpler and more understandable than other algorithms; it is described completely enough to meet the needs of a practical system;
it has several open-source implementations and is used
by several companies; its safety properties have been formally specified and proven; and its efficiency is comparable to other algorithms.
The remainder of the paper introduces the replicated
state machine problem (Section 2), discusses the strengths
and weaknesses of Paxos (Section 3), describes our general approach to understandability (Section 4), presents
the Raft consensus algorithm (Sections 5–7), evaluates
Raft (Section 8), and discusses related work (Section 9).
A few elements of the Raft algorithm have been omitted
here because of space limitations, but they are available in
an extended technical report [29]. The additional material
describes how clients interact with the system, and how
space in the Raft log can be reclaimed.

2 Replicated state machines
Consensus algorithms typically arise in the context of
replicated state machines [33]. In this approach, state machines on a collection of servers compute identical copies
of the same state and can continue operating even if some
of the servers are down. Replicated state machines are
used to solve a variety of fault tolerance problems in distributed systems. For example, large-scale systems that

2014 USENIX Annual Technical Conference 305

• In the common case, a command can complete as soon
as a majority of the cluster has responded to a single
round of remote procedure calls; a minority of slow
servers need not impact overall system performance.

3 What’s wrong with Paxos?

Figure 1: Replicated state machine architecture. The consensus algorithm manages a replicated log containing state
machine commands from clients. The state machines process
identical sequences of commands from the logs, so they produce the same outputs.

have a single cluster leader, such as GFS [7], HDFS [34],
and RAMCloud [30], typically use a separate replicated
state machine to manage leader election and store configuration information that must survive leader crashes. Examples of replicated state machines include Chubby [2]
and ZooKeeper [9].
Replicated state machines are typically implemented
using a replicated log, as shown in Figure 1. Each server
stores a log containing a series of commands, which its
state machine executes in order. Each log contains the
same commands in the same order, so each state machine processes the same sequence of commands. Since
the state machines are deterministic, each computes the
same state and the same sequence of outputs.
Keeping the replicated log consistent is the job of the
consensus algorithm. The consensus module on a server
receives commands from clients and adds them to its log.
It communicates with the consensus modules on other
servers to ensure that every log eventually contains the
same requests in the same order, even if some servers fail.
Once commands are properly replicated, each server’s
state machine processes them in log order, and the outputs are returned to clients. As a result, the servers appear
to form a single, highly reliable state machine.
Consensus algorithms for practical systems typically
have the following properties:
• They ensure safety (never returning an incorrect result)
under all non-Byzantine conditions, including network
delays, partitions, and packet loss, duplication, and reordering.
• They are fully functional (available) as long as any majority of the servers are operational and can communicate with each other and with clients. Thus, a typical
cluster of five servers can tolerate the failure of any two
servers. Servers are assumed to fail by stopping; they
may later recover from state on stable storage and rejoin the cluster.
• They do not depend on timing to ensure the consistency
of the logs: faulty clocks and extreme message delays
can, at worst, cause availability problems.

306 2014 USENIX Annual Technical Conference

Over the last ten years, Leslie Lamport’s Paxos protocol [13] has become almost synonymous with consensus:
it is the protocol most commonly taught in courses, and
most implementations of consensus use it as a starting
point. Paxos first defines a protocol capable of reaching
agreement on a single decision, such as a single replicated
log entry. We refer to this subset as single-decree Paxos.
Paxos then combines multiple instances of this protocol to
facilitate a series of decisions such as a log (multi-Paxos).
Paxos ensures both safety and liveness, and it supports
changes in cluster membership. Its correctness has been
proven, and it is efficient in the normal case.
Unfortunately, Paxos has two significant drawbacks.
The first drawback is that Paxos is exceptionally difficult to understand. The full explanation [13] is notoriously opaque; few people succeed in understanding it, and
only with great effort. As a result, there have been several
attempts to explain Paxos in simpler terms [14, 18, 19].
These explanations focus on the single-decree subset, yet
they are still challenging. In an informal survey of attendees at NSDI 2012, we found few people who were comfortable with Paxos, even among seasoned researchers.
We struggled with Paxos ourselves; we were not able to
understand the complete protocol until after reading several simplified explanations and designing our own alternative protocol, a process that took almost a year.
We hypothesize that Paxos’ opaqueness derives from
its choice of the single-decree subset as its foundation.
Single-decree Paxos is dense and subtle: it is divided into
two stages that do not have simple intuitive explanations
and cannot be understood independently. Because of this,
it is difficult to develop intuitions about why the singledecree protocol works. The composition rules for multiPaxos add significant additional complexity and subtlety.
We believe that the overall problem of reaching consensus
on multiple decisions (i.e., a log instead of a single entry)
can be decomposed in other ways that are more direct and
obvious.
The second problem with Paxos is that it does not provide a good foundation for building practical implementations. One reason is that there is no widely agreedupon algorithm for multi-Paxos. Lamport’s descriptions
are mostly about single-decree Paxos; he sketched possible approaches to multi-Paxos, but many details are missing. There have been several attempts to flesh out and optimize Paxos, such as [24], [35], and [11], but these differ
from each other and from Lamport’s sketches. Systems
such as Chubby [4] have implemented Paxos-like algorithms, but in most cases their details have not been pub-

USENIX Association

lished.
Furthermore, the Paxos architecture is a poor one for
building practical systems; this is another consequence of
the single-decree decomposition. For example, there is little benefit to choosing a collection of log entries independently and then melding them into a sequential log; this
just adds complexity. It is simpler and more efficient to
design a system around a log, where new entries are appended sequentially in a constrained order. Another problem is that Paxos uses a symmetric peer-to-peer approach
at its core (though it eventually suggests a weak form of
leadership as a performance optimization). This makes
sense in a simplified world where only one decision will
be made, but few practical systems use this approach. If a
series of decisions must be made, it is simpler and faster
to first elect a leader, then have the leader coordinate the
decisions.
As a result, practical systems bear little resemblance
to Paxos. Each implementation begins with Paxos, discovers the difficulties in implementing it, and then develops a significantly different architecture. This is timeconsuming and error-prone, and the difficulties of understanding Paxos exacerbate the problem. Paxos’ formulation may be a good one for proving theorems about its correctness, but real implementations are so different from
Paxos that the proofs have little value. The following comment from the Chubby implementers is typical:
There are significant gaps between the description of
the Paxos algorithm and the needs of a real-world
system. . . . the final system will be based on an unproven protocol [4].

Because of these problems, we concluded that Paxos
does not provide a good foundation either for system
building or for education. Given the importance of consensus in large-scale software systems, we decided to see
if we could design an alternative consensus algorithm
with better properties than Paxos. Raft is the result of that
experiment.

4 Designing for understandability
We had several goals in designing Raft: it must provide
a complete and practical foundation for system building,
so that it significantly reduces the amount of design work
required of developers; it must be safe under all conditions
and available under typical operating conditions; and it
must be efficient for common operations. But our most
important goal—and most difficult challenge—was understandability. It must be possible for a large audience to
understand the algorithm comfortably. In addition, it must
be possible to develop intuitions about the algorithm, so
that system builders can make the extensions that are inevitable in real-world implementations.
There were numerous points in the design of Raft
where we had to choose among alternative approaches.
In these situations we evaluated the alternatives based on

USENIX Association

understandability: how hard is it to explain each alternative (for example, how complex is its state space, and does
it have subtle implications?), and how easy will it be for a
reader to completely understand the approach and its implications?
We recognize that there is a high degree of subjectivity in such analysis; nonetheless, we used two techniques
that are generally applicable. The first technique is the
well-known approach of problem decomposition: wherever possible, we divided problems into separate pieces
that could be solved, explained, and understood relatively
independently. For example, in Raft we separated leader
election, log replication, safety, and membership changes.
Our second approach was to simplify the state space
by reducing the number of states to consider, making the
system more coherent and eliminating nondeterminism
where possible. Specifically, logs are not allowed to have
holes, and Raft limits the ways in which logs can become
inconsistent with each other. Although in most cases we
tried to eliminate nondeterminism, there are some situations where nondeterminism actually improves understandability. In particular, randomized approaches introduce nondeterminism, but they tend to reduce the state
space by handling all possible choices in a similar fashion
(“choose any; it doesn’t matter”). We used randomization
to simplify the Raft leader election algorithm.

5 The Raft consensus algorithm
Raft is an algorithm for managing a replicated log of
the form described in Section 2. Figure 2 summarizes the
algorithm in condensed form for reference, and Figure 3
lists key properties of the algorithm; the elements of these
figures are discussed piecewise over the rest of this section.
Raft implements consensus by first electing a distinguished leader, then giving the leader complete responsibility for managing the replicated log. The leader accepts
log entries from clients, replicates them on other servers,
and tells servers when it is safe to apply log entries to
their state machines. Having a leader simplifies the management of the replicated log. For example, the leader can
decide where to place new entries in the log without consulting other servers, and data flows in a simple fashion
from the leader to other servers. A leader can fail or become disconnected from the other servers, in which case
a new leader is elected.
Given the leader approach, Raft decomposes the consensus problem into three relatively independent subproblems, which are discussed in the subsections that follow:
• Leader election: a new leader must be chosen when an
existing leader fails (Section 5.2).
• Log replication: the leader must accept log entries
from clients and replicate them across the cluster, forcing the other logs to agree with its own (Section 5.3).
• Safety: the key safety property for Raft is the State Ma-

2014 USENIX Annual Technical Conference 307

State
Persistent state on all servers:
(Updated on stable storage before responding to RPCs)
currentTerm
latest term server has seen (initialized to 0
on first boot, increases monotonically)
votedFor
candidateId that received vote in current
term (or null if none)
log[]
log entries; each entry contains command
for state machine, and term when entry
was received by leader (first index is 1)
Volatile state on all servers:
commitIndex
index of highest log entry known to be
committed (initialized to 0, increases
monotonically)
lastApplied
index of highest log entry applied to state
machine (initialized to 0, increases
monotonically)
Volatile state on leaders:
(Reinitialized after election)
nextIndex[]
for each server, index of the next log entry
to send to that server (initialized to leader
last log index + 1)
matchIndex[]
for each server, index of highest log entry
known to be replicated on server
(initialized to 0, increases monotonically)

AppendEntries RPC
Invoked by leader to replicate log entries (§5.3); also used as
heartbeat (§5.2).
Arguments:
term
leaderId
prevLogIndex
prevLogTerm
entries[]
leaderCommit
Results:
term
success

leader’s term
so follower can redirect clients
index of log entry immediately preceding
new ones
term of prevLogIndex entry
log entries to store (empty for heartbeat;
may send more than one for efficiency)
leader’s commitIndex
currentTerm, for leader to update itself
true if follower contained entry matching
prevLogIndex and prevLogTerm

Receiver implementation:
1. Reply false if term < currentTerm (§5.1)
2. Reply false if log doesn’t contain an entry at prevLogIndex
whose term matches prevLogTerm (§5.3)
3. If an existing entry conflicts with a new one (same index
but different terms), delete the existing entry and all that
follow it (§5.3)
4. Append any new entries not already in the log
5. If leaderCommit > commitIndex, set commitIndex =
min(leaderCommit, index of last new entry)

RequestVote RPC
Invoked by candidates to gather votes (§5.2).
Arguments:
term
candidateId
lastLogIndex
lastLogTerm

candidate’s term
candidate requesting vote
index of candidate’s last log entry (§5.4)
term of candidate’s last log entry (§5.4)

Results:
term
voteGranted

currentTerm, for candidate to update itself
true means candidate received vote

Receiver implementation:
1. Reply false if term < currentTerm (§5.1)
2. If votedFor is null or candidateId, and candidate’s log is at
least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)

Rules for Servers
All Servers:
• If commitIndex > lastApplied: increment lastApplied, apply
log[lastApplied] to state machine (§5.3)
• If RPC request or response contains term T > currentTerm:
set currentTerm = T, convert to follower (§5.1)
Followers (§5.2):
• Respond to RPCs from candidates and leaders
• If election timeout elapses without receiving AppendEntries
RPC from current leader or granting vote to candidate:
convert to candidate
Candidates (§5.2):
• On conversion to candidate, start election:
• Increment currentTerm
• Vote for self
• Reset election timer
• Send RequestVote RPCs to all other servers
• If votes received from majority of servers: become leader
• If AppendEntries RPC received from new leader: convert to
follower
• If election timeout elapses: start new election
Leaders:
• Upon election: send initial empty AppendEntries RPCs
(heartbeat) to each server; repeat during idle periods to
prevent election timeouts (§5.2)
• If command received from client: append entry to local log,
respond after entry applied to state machine (§5.3)
• If last log index ≥ nextIndex for a follower: send
AppendEntries RPC with log entries starting at nextIndex
• If successful: update nextIndex and matchIndex for
follower (§5.3)
• If AppendEntries fails because of log inconsistency:
decrement nextIndex and retry (§5.3)
• If there exists an N such that N > commitIndex, a majority
of matchIndex[i] ≥ N, and log[N].term == currentTerm:
set commitIndex = N (§5.3, §5.4).

Figure 2: A condensed summary of the Raft consensus algorithm (excluding membership changes and log compaction). The server
behavior in the upper-left box is described as a set of rules that trigger independently and repeatedly. Section numbers such as §5.2
indicate where particular features are discussed. A formal specification [28] describes the algorithm more precisely.

308 2014 USENIX Annual Technical Conference

USENIX Association

Election Safety: at most one leader can be elected in a
given term. §5.2
Leader Append-Only: a leader never overwrites or deletes
entries in its log; it only appends new entries. §5.3
Log Matching: if two logs contain an entry with the same
index and term, then the logs are identical in all entries up
through the given index. §5.3
Leader Completeness: if a log entry is committed in a
given term, then that entry will be present in the logs of
the leaders for all higher-numbered terms. §5.4
State Machine Safety: if a server has applied a log entry at
a given index to its state machine, no other server will ever
apply a different log entry for the same index. §5.4.3

Figure 4: Server states. Followers only respond to requests
from other servers. If a follower receives no communication,
it becomes a candidate and initiates an election. A candidate
that receives votes from a majority of the full cluster becomes
the new leader. Leaders typically operate until they fail.

Figure 3: Raft guarantees that each of these properties is true
at all times. The section numbers indicate where each property is discussed.

chine Safety Property in Figure 3: if any server has applied a particular log entry to its state machine, then
no other server may apply a different command for the
same log index. Section 5.4 describes how Raft ensures
this property; the solution involves an additional restriction on the election mechanism described in Section 5.2.
After presenting the consensus algorithm, this section discusses the issue of availability and the role of timing in the
system.
5.1 Raft basics
A Raft cluster contains several servers; five is a typical
number, which allows the system to tolerate two failures.
At any given time each server is in one of three states:
leader, follower, or candidate. In normal operation there
is exactly one leader and all of the other servers are followers. Followers are passive: they issue no requests on
their own but simply respond to requests from leaders
and candidates. The leader handles all client requests (if
a client contacts a follower, the follower redirects it to the
leader). The third state, candidate, is used to elect a new
leader as described in Section 5.2. Figure 4 shows the
states and their transitions; the transitions are discussed
below.
Raft divides time into terms of arbitrary length, as
shown in Figure 5. Terms are numbered with consecutive
integers. Each term begins with an election, in which one
or more candidates attempt to become leader as described
in Section 5.2. If a candidate wins the election, then it
serves as leader for the rest of the term. In some situations
an election will result in a split vote. In this case the term
will end with no leader; a new term (with a new election)
will begin shortly. Raft ensures that there is at most one
leader in a given term.
Different servers may observe the transitions between
terms at different times, and in some situations a server
may not observe an election or even entire terms. Terms

USENIX Association

Figure 5: Time is divided into terms, and each term begins
with an election. After a successful election, a single leader
manages the cluster until the end of the term. Some elections
fail, in which case the term ends without choosing a leader.
The transitions between terms may be observed at different
times on different servers.

act as a logical clock [12] in Raft, and they allow servers
to detect obsolete information such as stale leaders. Each
server stores a current term number, which increases
monotonically over time. Current terms are exchanged
whenever servers communicate; if one server’s current
term is smaller than the other’s, then it updates its current
term to the larger value. If a candidate or leader discovers
that its term is out of date, it immediately reverts to follower state. If a server receives a request with a stale term
number, it rejects the request.
Raft servers communicate using remote procedure calls
(RPCs), and the consensus algorithm requires only two
types of RPCs. RequestVote RPCs are initiated by candidates during elections (Section 5.2), and AppendEntries
RPCs are initiated by leaders to replicate log entries and
to provide a form of heartbeat (Section 5.3). Servers retry
RPCs if they do not receive a response in a timely manner,
and they issue RPCs in parallel for best performance.
5.2 Leader election
Raft uses a heartbeat mechanism to trigger leader election. When servers start up, they begin as followers. A
server remains in follower state as long as it receives valid
RPCs from a leader or candidate. Leaders send periodic
heartbeats (AppendEntries RPCs that carry no log entries)
to all followers in order to maintain their authority. If a
follower receives no communication over a period of time
called the election timeout, then it assumes there is no viable leader and begins an election to choose a new leader.
To begin an election, a follower increments its current
term and transitions to candidate state. It then votes for

2014 USENIX Annual Technical Conference 309

itself and issues RequestVote RPCs in parallel to each of
the other servers in the cluster. A candidate continues in
this state until one of three things happens: (a) it wins the
election, (b) another server establishes itself as leader, or
(c) a period of time goes by with no winner. These outcomes are discussed separately in the paragraphs below.
A candidate wins an election if it receives votes from
a majority of the servers in the full cluster for the same
term. Each server will vote for at most one candidate in a
given term, on a first-come-first-served basis (note: Section 5.4 adds an additional restriction on votes). The majority rule ensures that at most one candidate can win the
election for a particular term (the Election Safety Property in Figure 3). Once a candidate wins an election, it
becomes leader. It then sends heartbeat messages to all of
the other servers to establish its authority and prevent new
elections.
While waiting for votes, a candidate may receive an
AppendEntries RPC from another server claiming to be
leader. If the leader’s term (included in its RPC) is at least
as large as the candidate’s current term, then the candidate
recognizes the leader as legitimate and returns to follower
state. If the term in the RPC is smaller than the candidate’s
current term, then the candidate rejects the RPC and continues in candidate state.
The third possible outcome is that a candidate neither
wins nor loses the election: if many followers become
candidates at the same time, votes could be split so that
no candidate obtains a majority. When this happens, each
candidate will time out and start a new election by incrementing its term and initiating another round of RequestVote RPCs. However, without extra measures split votes
could repeat indefinitely.
Raft uses randomized election timeouts to ensure that
split votes are rare and that they are resolved quickly. To
prevent split votes in the first place, election timeouts are
chosen randomly from a fixed interval (e.g., 150–300ms).
This spreads out the servers so that in most cases only a
single server will time out; it wins the election and sends
heartbeats before any other servers time out. The same
mechanism is used to handle split votes. Each candidate
restarts its randomized election timeout at the start of an
election, and it waits for that timeout to elapse before
starting the next election; this reduces the likelihood of
another split vote in the new election. Section 8.3 shows
that this approach elects a leader rapidly.
Elections are an example of how understandability
guided our choice between design alternatives. Initially
we planned to use a ranking system: each candidate was
assigned a unique rank, which was used to select between
competing candidates. If a candidate discovered another
candidate with higher rank, it would return to follower
state so that the higher ranking candidate could more easily win the next election. We found that this approach

310 2014 USENIX Annual Technical Conference

Figure 6: Logs are composed of entries, which are numbered
sequentially. Each entry contains the term in which it was
created (the number in each box) and a command for the state
machine. An entry is considered committed if it is safe for that
entry to be applied to state machines.

created subtle issues around availability (a lower-ranked
server might need to time out and become a candidate
again if a higher-ranked server fails, but if it does so too
soon, it can reset progress towards electing a leader). We
made adjustments to the algorithm several times, but after
each adjustment new corner cases appeared. Eventually
we concluded that the randomized retry approach is more
obvious and understandable.
5.3 Log replication
Once a leader has been elected, it begins servicing
client requests. Each client request contains a command to
be executed by the replicated state machines. The leader
appends the command to its log as a new entry, then issues AppendEntries RPCs in parallel to each of the other
servers to replicate the entry. When the entry has been
safely replicated (as described below), the leader applies
the entry to its state machine and returns the result of that
execution to the client. If followers crash or run slowly,
or if network packets are lost, the leader retries AppendEntries RPCs indefinitely (even after it has responded to
the client) until all followers eventually store all log entries.
Logs are organized as shown in Figure 6. Each log entry stores a state machine command along with the term
number when the entry was received by the leader. The
term numbers in log entries are used to detect inconsistencies between logs and to ensure some of the properties
in Figure 3. Each log entry also has an integer index identifying its position in the log.
The leader decides when it is safe to apply a log entry to the state machines; such an entry is called committed. Raft guarantees that committed entries are durable
and will eventually be executed by all of the available
state machines. A log entry is committed once the leader
that created the entry has replicated it on a majority of
the servers (e.g., entry 7 in Figure 6). This also commits

USENIX Association

all preceding entries in the leader’s log, including entries
created by previous leaders. Section 5.4 discusses some
subtleties when applying this rule after leader changes,
and it also shows that this definition of commitment is
safe. The leader keeps track of the highest index it knows
to be committed, and it includes that index in future
AppendEntries RPCs (including heartbeats) so that the
other servers eventually find out. Once a follower learns
that a log entry is committed, it applies the entry to its
local state machine (in log order).
We designed the Raft log mechanism to maintain a high
level of coherency between the logs on different servers.
Not only does this simplify the system’s behavior and
make it more predictable, but it is an important component
of ensuring safety. Raft maintains the following properties, which together constitute the Log Matching Property
in Figure 3:
• If two entries in different logs have the same index and
term, then they store the same command.
• If two entries in different logs have the same index and
term, then the logs are identical in all preceding entries.
The first property follows from the fact that a leader
creates at most one entry with a given log index in a given
term, and log entries never change their position in the
log. The second property is guaranteed by a simple consistency check performed by AppendEntries. When sending an AppendEntries RPC, the leader includes the index
and term of the entry in its log that immediately precedes
the new entries. If the follower does not find an entry in
its log with the same index and term, then it refuses the
new entries. The consistency check acts as an induction
step: the initial empty state of the logs satisfies the Log
Matching Property, and the consistency check preserves
the Log Matching Property whenever logs are extended.
As a result, whenever AppendEntries returns successfully,
the leader knows that the follower’s log is identical to its
own log up through the new entries.
During normal operation, the logs of the leader and
followers stay consistent, so the AppendEntries consistency check never fails. However, leader crashes can leave
the logs inconsistent (the old leader may not have fully
replicated all of the entries in its log). These inconsistencies can compound over a series of leader and follower
crashes. Figure 7 illustrates the ways in which followers’
logs may differ from that of a new leader. A follower may
be missing entries that are present on the leader, it may
have extra entries that are not present on the leader, or
both. Missing and extraneous entries in a log may span
multiple terms.
In Raft, the leader handles inconsistencies by forcing
the followers’ logs to duplicate its own. This means that
conflicting entries in follower logs will be overwritten
with entries from the leader’s log. Section 5.4 will show
that this is safe when coupled with one more restriction.

USENIX Association

Figure 7: When the leader at the top comes to power, it is
possible that any of scenarios (a–f) could occur in follower
logs. Each box represents one log entry; the number in the
box is its term. A follower may be missing entries (a–b), may
have extra uncommitted entries (c–d), or both (e–f). For example, scenario (f) could occur if that server was the leader
for term 2, added several entries to its log, then crashed before
committing any of them; it restarted quickly, became leader
for term 3, and added a few more entries to its log; before any
of the entries in either term 2 or term 3 were committed, the
server crashed again and remained down for several terms.

To bring a follower’s log into consistency with its own,
the leader must find the latest log entry where the two
logs agree, delete any entries in the follower’s log after
that point, and send the follower all of the leader’s entries
after that point. All of these actions happen in response
to the consistency check performed by AppendEntries
RPCs. The leader maintains a nextIndex for each follower,
which is the index of the next log entry the leader will
send to that follower. When a leader first comes to power,
it initializes all nextIndex values to the index just after the
last one in its log (11 in Figure 7). If a follower’s log is
inconsistent with the leader’s, the AppendEntries consistency check will fail in the next AppendEntries RPC. After a rejection, the leader decrements nextIndex and retries
the AppendEntries RPC. Eventually nextIndex will reach
a point where the leader and follower logs match. When
this happens, AppendEntries will succeed, which removes
any conflicting entries in the follower’s log and appends
entries from the leader’s log (if any). Once AppendEntries
succeeds, the follower’s log is consistent with the leader’s,
and it will remain that way for the rest of the term.
The protocol can be optimized to reduce the number of
rejected AppendEntries RPCs; see [29] for details.
With this mechanism, a leader does not need to take any
special actions to restore log consistency when it comes to
power. It just begins normal operation, and the logs automatically converge in response to failures of the AppendEntries consistency check. A leader never overwrites or
deletes entries in its own log (the Leader Append-Only
Property in Figure 3).
This log replication mechanism exhibits the desirable
consensus properties described in Section 2: Raft can accept, replicate, and apply new log entries as long as a ma-

2014 USENIX Annual Technical Conference 311

jority of the servers are up; in the normal case a new entry
can be replicated with a single round of RPCs to a majority of the cluster; and a single slow follower will not
impact performance.
5.4 Safety
The previous sections described how Raft elects leaders and replicates log entries. However, the mechanisms
described so far are not quite sufficient to ensure that each
state machine executes exactly the same commands in the
same order. For example, a follower might be unavailable
while the leader commits several log entries, then it could
be elected leader and overwrite these entries with new
ones; as a result, different state machines might execute
different command sequences.
This section completes the Raft algorithm by adding a
restriction on which servers may be elected leader. The
restriction ensures that the leader for any given term contains all of the entries committed in previous terms (the
Leader Completeness Property from Figure 3). Given the
election restriction, we then make the rules for commitment more precise. Finally, we present a proof sketch for
the Leader Completeness Property and show how it leads
to correct behavior of the replicated state machine.
5.4.1 Election restriction
In any leader-based consensus algorithm, the leader
must eventually store all of the committed log entries. In
some consensus algorithms, such as Viewstamped Replication [20], a leader can be elected even if it doesn’t
initially contain all of the committed entries. These algorithms contain additional mechanisms to identify the
missing entries and transmit them to the new leader, either during the election process or shortly afterwards. Unfortunately, this results in considerable additional mechanism and complexity. Raft uses a simpler approach where
it guarantees that all the committed entries from previous
terms are present on each new leader from the moment of
its election, without the need to transfer those entries to
the leader. This means that log entries only flow in one direction, from leaders to followers, and leaders never overwrite existing entries in their logs.
Raft uses the voting process to prevent a candidate from
winning an election unless its log contains all committed
entries. A candidate must contact a majority of the cluster
in order to be elected, which means that every committed
entry must be present in at least one of those servers. If the
candidate’s log is at least as up-to-date as any other log
in that majority (where “up-to-date” is defined precisely
below), then it will hold all the committed entries. The
RequestVote RPC implements this restriction: the RPC
includes information about the candidate’s log, and the
voter denies its vote if its own log is more up-to-date than
that of the candidate.
Raft determines which of two logs is more up-to-date
by comparing the index and term of the last entries in the

312 2014 USENIX Annual Technical Conference

Figure 8: A time sequence showing why a leader cannot determine commitment using log entries from older terms. In
(a) S1 is leader and partially replicates the log entry at index
2. In (b) S1 crashes; S5 is elected leader for term 3 with votes
from S3, S4, and itself, and accepts a different entry at log
index 2. In (c) S5 crashes; S1 restarts, is elected leader, and
continues replication. At this point, the log entry from term 2
has been replicated on a majority of the servers, but it is not
committed. If S1 crashes as in (d), S5 could be elected leader
(with votes from S2, S3, and S4) and overwrite the entry with
its own entry from term 3. However, if S1 replicates an entry from its current term on a majority of the servers before
crashing, as in (e), then this entry is committed (S5 cannot
win an election). At this point all preceding entries in the log
are committed as well.

logs. If the logs have last entries with different terms, then
the log with the later term is more up-to-date. If the logs
end with the same term, then whichever log is longer is
more up-to-date.
5.4.2 Committing entries from previous terms
As described in Section 5.3, a leader knows that an entry from its current term is committed once that entry is
stored on a majority of the servers. If a leader crashes before committing an entry, future leaders will attempt to
finish replicating the entry. However, a leader cannot immediately conclude that an entry from a previous term is
committed once it is stored on a majority of servers. Figure 8 illustrates a situation where an old log entry is stored
on a majority of servers, yet can still be overwritten by a
future leader.
To eliminate problems like the one in Figure 8, Raft
never commits log entries from previous terms by counting replicas. Only log entries from the leader’s current
term are committed by counting replicas; once an entry
from the current term has been committed in this way,
then all prior entries are committed indirectly because
of the Log Matching Property. There are some situations
where a leader could safely conclude that an older log entry is committed (for example, if that entry is stored on every server), but Raft takes a more conservative approach
for simplicity.
Raft incurs this extra complexity in the commitment
rules because log entries retain their original term numbers when a leader replicates entries from previous
terms. In other consensus algorithms, if a new leader rereplicates entries from prior “terms,” it must do so with
its new “term number.” Raft’s approach makes it easier

USENIX Association

Figure 9: If S1 (leader for term T) commits a new log entry
from its term, and S5 is elected leader for a later term U, then
there must be at least one server (S3) that accepted the log
entry and also voted for S5.

to reason about log entries, since they maintain the same
term number over time and across logs. In addition, new
leaders in Raft send fewer log entries from previous terms
than in other algorithms (other algorithms must send redundant log entries to renumber them before they can be
committed).
5.4.3 Safety argument
Given the complete Raft algorithm, we can now argue more precisely that the Leader Completeness Property holds (this argument is based on the safety proof; see
Section 8.2). We assume that the Leader Completeness
Property does not hold, then we prove a contradiction.
Suppose the leader for term T (leaderT ) commits a log
entry from its term, but that log entry is not stored by the
leader of some future term. Consider the smallest term U
> T whose leader (leaderU ) does not store the entry.
1. The committed entry must have been absent from
leaderU ’s log at the time of its election (leaders never
delete or overwrite entries).
2. leaderT replicated the entry on a majority of the
cluster, and leaderU received votes from a majority of the
cluster. Thus, at least one server (“the voter”) both accepted the entry from leaderT and voted for leaderU , as
shown in Figure 9. The voter is key to reaching a contradiction.
3. The voter must have accepted the committed entry
from leaderT before voting for leaderU ; otherwise it would
have rejected the AppendEntries request from leaderT (its
current term would have been higher than T).
4. The voter still stored the entry when it voted for
leaderU , since every intervening leader contained the entry (by assumption), leaders never remove entries, and followers only remove entries if they conflict with the leader.
5. The voter granted its vote to leaderU , so leaderU ’s
log must have been as up-to-date as the voter’s. This leads
to one of two contradictions.
6. First, if the voter and leaderU shared the same last
log term, then leaderU ’s log must have been at least as
long as the voter’s, so its log contained every entry in the
voter’s log. This is a contradiction, since the voter contained the committed entry and leaderU was assumed not
to.
7. Otherwise, leaderU ’s last log term must have been

USENIX Association

larger than the voter’s. Moreover, it was larger than T,
since the voter’s last log term was at least T (it contains
the committed entry from term T). The earlier leader that
created leaderU ’s last log entry must have contained the
committed entry in its log (by assumption). Then, by the
Log Matching Property, leaderU ’s log must also contain
the committed entry, which is a contradiction.
8. This completes the contradiction. Thus, the leaders
of all terms greater than T must contain all entries from
term T that are committed in term T.
9. The Log Matching Property guarantees that future
leaders will also contain entries that are committed indirectly, such as index 2 in Figure 8(d).
Given the Leader Completeness Property, it is easy to
prove the State Machine Safety Property from Figure 3
and that all state machines apply the same log entries in
the same order (see [29]).
5.5 Follower and candidate crashes
Until this point we have focused on leader failures. Follower and candidate crashes are much simpler to handle than leader crashes, and they are both handled in the
same way. If a follower or candidate crashes, then future RequestVote and AppendEntries RPCs sent to it will
fail. Raft handles these failures by retrying indefinitely;
if the crashed server restarts, then the RPC will complete
successfully. If a server crashes after completing an RPC
but before responding, then it will receive the same RPC
again after it restarts. Raft RPCs are idempotent, so this
causes no harm. For example, if a follower receives an
AppendEntries request that includes log entries already
present in its log, it ignores those entries in the new request.
5.6 Timing and availability
One of our requirements for Raft is that safety must
not depend on timing: the system must not produce incorrect results just because some event happens more quickly
or slowly than expected. However, availability (the ability
of the system to respond to clients in a timely manner)
must inevitably depend on timing. For example, if message exchanges take longer than the typical time between
server crashes, candidates will not stay up long enough to
win an election; without a steady leader, Raft cannot make
progress.
Leader election is the aspect of Raft where timing is
most critical. Raft will be able to elect and maintain a
steady leader as long as the system satisfies the following timing requirement:
broadcastTime ≪ electionTimeout ≪ MTBF
In this inequality broadcastTime is the average time it
takes a server to send RPCs in parallel to every server
in the cluster and receive their responses; electionTimeout is the election timeout described in Section 5.2; and
MTBF is the average time between failures for a single

2014 USENIX Annual Technical Conference 313

server. The broadcast time should be an order of magnitude less than the election timeout so that leaders can
reliably send the heartbeat messages required to keep followers from starting elections; given the randomized approach used for election timeouts, this inequality also
makes split votes unlikely. The election timeout should be
a few orders of magnitude less than MTBF so that the system makes steady progress. When the leader crashes, the
system will be unavailable for roughly the election timeout; we would like this to represent only a small fraction
of overall time.
The broadcast time and MTBF are properties of the underlying system, while the election timeout is something
we must choose. Raft’s RPCs typically require the recipient to persist information to stable storage, so the broadcast time may range from 0.5ms to 20ms, depending on
storage technology. As a result, the election timeout is
likely to be somewhere between 10ms and 500ms. Typical
server MTBFs are several months or more, which easily
satisfies the timing requirement.

6 Cluster membership changes
Up until now we have assumed that the cluster configuration (the set of servers participating in the consensus
algorithm) is fixed. In practice, it will occasionally be necessary to change the configuration, for example to replace
servers when they fail or to change the degree of replication. Although this can be done by taking the entire cluster
off-line, updating configuration files, and then restarting
the cluster, this would leave the cluster unavailable during the changeover. In addition, if there are any manual
steps, they risk operator error. In order to avoid these issues, we decided to automate configuration changes and
incorporate them into the Raft consensus algorithm.
For the configuration change mechanism to be safe,
there must be no point during the transition where it
is possible for two leaders to be elected for the same
term. Unfortunately, any approach where servers switch
directly from the old configuration to the new configuration is unsafe. It isn’t possible to atomically switch all of
the servers at once, so the cluster can potentially split into
two independent majorities during the transition (see Figure 10).
In order to ensure safety, configuration changes must
use a two-phase approach. There are a variety of ways
to implement the two phases. For example, some systems
(e.g., [20]) use the first phase to disable the old configuration so it cannot process client requests; then the second
phase enables the new configuration. In Raft the cluster
first switches to a transitional configuration we call joint
consensus; once the joint consensus has been committed,
the system then transitions to the new configuration. The
joint consensus combines both the old and new configurations:
• Log entries are replicated to all servers in both config-

314 2014 USENIX Annual Technical Conference

Figure 10: Switching directly from one configuration to another is unsafe because different servers will switch at different times. In this example, the cluster grows from three
servers to five. Unfortunately, there is a point in time where
two different leaders can be elected for the same term, one
with a majority of the old configuration (Cold ) and another
with a majority of the new configuration (Cnew ).

urations.
• Any server from either configuration may serve as
leader.
• Agreement (for elections and entry commitment) requires separate majorities from both the old and new
configurations.
The joint consensus allows individual servers to transition
between configurations at different times without compromising safety. Furthermore, joint consensus allows the
cluster to continue servicing client requests throughout
the configuration change.
Cluster configurations are stored and communicated
using special entries in the replicated log; Figure 11 illustrates the configuration change process. When the leader
receives a request to change the configuration from Cold
to Cnew , it stores the configuration for joint consensus
(Cold,new in the figure) as a log entry and replicates that
entry using the mechanisms described previously. Once a
given server adds the new configuration entry to its log,
it uses that configuration for all future decisions (a server
always uses the latest configuration in its log, regardless
of whether the entry is committed). This means that the
leader will use the rules of Cold,new to determine when the
log entry for Cold,new is committed. If the leader crashes,
a new leader may be chosen under either Cold or Cold,new ,
depending on whether the winning candidate has received
Cold,new . In any case, Cnew cannot make unilateral decisions during this period.
Once Cold,new has been committed, neither Cold nor Cnew
can make decisions without approval of the other, and the
Leader Completeness Property ensures that only servers
with the Cold,new log entry can be elected as leader. It is
now safe for the leader to create a log entry describing
Cnew and replicate it to the cluster. Again, this configuration will take effect on each server as soon as it is seen.
When the new configuration has been committed under
the rules of Cnew , the old configuration is irrelevant and
servers not in the new configuration can be shut down. As

USENIX Association

This does not affect normal elections, where each server
waits at least a minimum election timeout before starting
an election. However, it helps avoid disruptions from removed servers: if a leader is able to get heartbeats to its
cluster, then it will not be deposed by larger term numbers.

7 Clients and log compaction
Figure 11: Timeline for a configuration change. Dashed lines
show configuration entries that have been created but not
committed, and solid lines show the latest committed configuration entry. The leader first creates the Cold,new configuration
entry in its log and commits it to Cold,new (a majority of Cold
and a majority of Cnew ). Then it creates the Cnew entry and
commits it to a majority of Cnew . There is no point in time in
which Cold and Cnew can both make decisions independently.

shown in Figure 11, there is no time when Cold and Cnew
can both make unilateral decisions; this guarantees safety.
There are three more issues to address for reconfiguration. The first issue is that new servers may not initially
store any log entries. If they are added to the cluster in
this state, it could take quite a while for them to catch
up, during which time it might not be possible to commit new log entries. In order to avoid availability gaps,
Raft introduces an additional phase before the configuration change, in which the new servers join the cluster
as non-voting members (the leader replicates log entries
to them, but they are not considered for majorities). Once
the new servers have caught up with the rest of the cluster,
the reconfiguration can proceed as described above.
The second issue is that the cluster leader may not be
part of the new configuration. In this case, the leader steps
down (returns to follower state) once it has committed the
Cnew log entry. This means that there will be a period of
time (while it is committing Cnew ) when the leader is managing a cluster that does not include itself; it replicates log
entries but does not count itself in majorities. The leader
transition occurs when Cnew is committed because this is
the first point when the new configuration can operate independently (it will always be possible to choose a leader
from Cnew ). Before this point, it may be the case that only
a server from Cold can be elected leader.
The third issue is that removed servers (those not in
Cnew ) can disrupt the cluster. These servers will not receive heartbeats, so they will time out and start new elections. They will then send RequestVote RPCs with new
term numbers, and this will cause the current leader to
revert to follower state. A new leader will eventually be
elected, but the removed servers will time out again and
the process will repeat, resulting in poor availability.
To prevent this problem, servers disregard RequestVote
RPCs when they believe a current leader exists. Specifically, if a server receives a RequestVote RPC within
the minimum election timeout of hearing from a current leader, it does not update its term or grant its vote.

USENIX Association

This section has been omitted due to space limitations,
but the material is available in the extended version of this
paper [29]. It describes how clients interact with Raft, including how clients find the cluster leader and how Raft
supports linearizable semantics [8]. The extended version
also describes how space in the replicated log can be reclaimed using a snapshotting approach. These issues apply to all consensus-based systems, and Raft’s solutions
are similar to other systems.

8 Implementation and evaluation
We have implemented Raft as part of a replicated
state machine that stores configuration information for
RAMCloud [30] and assists in failover of the RAMCloud
coordinator. The Raft implementation contains roughly
2000 lines of C++ code, not including tests, comments, or
blank lines. The source code is freely available [21]. There
are also about 25 independent third-party open source implementations [31] of Raft in various stages of development, based on drafts of this paper. Also, various companies are deploying Raft-based systems [31].
The remainder of this section evaluates Raft using three
criteria: understandability, correctness, and performance.
8.1 Understandability
To measure Raft’s understandability relative to Paxos,
we conducted an experimental study using upper-level undergraduate and graduate students in an Advanced Operating Systems course at Stanford University and a Distributed Computing course at U.C. Berkeley. We recorded
a video lecture of Raft and another of Paxos, and created
corresponding quizzes. The Raft lecture covered the content of this paper; the Paxos lecture covered enough material to create an equivalent replicated state machine, including single-decree Paxos, multi-decree Paxos, reconfiguration, and a few optimizations needed in practice
(such as leader election). The quizzes tested basic understanding of the algorithms and also required students
to reason about corner cases. Each student watched one
video, took the corresponding quiz, watched the second
video, and took the second quiz. About half of the participants did the Paxos portion first and the other half did
the Raft portion first in order to account for both individual differences in performance and experience gained
from the first portion of the study. We compared participants’ scores on each quiz to determine whether participants showed a better understanding of Raft.
We tried to make the comparison between Paxos and

2014 USENIX Annual Technical Conference 315

number of participants

60
50

Raft grade

40
30

20
15

Paxos much easier
Paxos somewhat easier
Roughly equal
Raft somewhat easier
Raft much easier

10
5
0
implement

20
10
Raft then Paxos
Paxos then Raft
0
0

10

20
30
40
Paxos grade

50

explain

Figure 13: Using a 5-point scale, participants were asked
(left) which algorithm they felt would be easier to implement
in a functioning, correct, and efficient system, and (right)
which would be easier to explain to a CS graduate student.
60

Figure 12: A scatter plot comparing 43 participants’ performance on the Raft and Paxos quizzes. Points above the diagonal (33) represent participants who scored higher for Raft.

Raft as fair as possible. The experiment favored Paxos in
two ways: 15 of the 43 participants reported having some
prior experience with Paxos, and the Paxos video is 14%
longer than the Raft video. As summarized in Table 1, we
have taken steps to mitigate potential sources of bias. All
of our materials are available for review [26, 28].
On average, participants scored 4.9 points higher on the
Raft quiz than on the Paxos quiz (out of a possible 60
points, the mean Raft score was 25.7 and the mean Paxos
score was 20.8); Figure 12 shows their individual scores.
A paired t-test states that, with 95% confidence, the true
distribution of Raft scores has a mean at least 2.5 points
larger than the true distribution of Paxos scores.
We also created a linear regression model that predicts
a new student’s quiz scores based on three factors: which
quiz they took, their degree of prior Paxos experience, and
the order in which they learned the algorithms. The model
predicts that the choice of quiz produces a 12.5-point difference in favor of Raft. This is significantly higher than
the observed difference of 4.9 points, because many of the
actual students had prior Paxos experience, which helped
Paxos considerably, whereas it helped Raft slightly less.
Curiously, the model also predicts scores 6.3 points lower
on Raft for people that have already taken the Paxos quiz;
although we don’t know why, this does appear to be statistically significant.
We also surveyed participants after their quizzes to see
which algorithm they felt would be easier to implement
or explain; these results are shown in Figure 13. An overwhelming majority of participants reported Raft would be
easier to implement and explain (33 of 41 for each question). However, these self-reported feelings may be less

reliable than participants’ quiz scores, and participants
may have been biased by knowledge of our hypothesis
that Raft is easier to understand.
A detailed discussion of the Raft user study is available
at [28].
8.2 Correctness
We have developed a formal specification and a proof
of safety for the consensus mechanism described in Section 5. The formal specification [28] makes the information summarized in Figure 2 completely precise using the
TLA+ specification language [15]. It is about 400 lines
long and serves as the subject of the proof. It is also useful on its own for anyone implementing Raft. We have
mechanically proven the Log Completeness Property using the TLA proof system [6]. However, this proof relies
on invariants that have not been mechanically checked
(for example, we have not proven the type safety of the
specification). Furthermore, we have written an informal
proof [28] of the State Machine Safety property which
is complete (it relies on the specification alone) and relatively precise (it is about 3500 words long).
8.3 Performance
Raft’s performance is similar to other consensus algorithms such as Paxos. The most important case for performance is when an established leader is replicating new
log entries. Raft achieves this using the minimal number
of messages (a single round-trip from the leader to half the
cluster). It is also possible to further improve Raft’s performance. For example, it easily supports batching and
pipelining requests for higher throughput and lower latency. Various optimizations have been proposed in the
literature for other algorithms; many of these could be applied to Raft, but we leave this to future work.
We used our Raft implementation to measure the performance of Raft’s leader election algorithm and answer
two questions. First, does the election process converge

Concern
Equal lecture quality

Steps taken to mitigate bias
Materials for review [26, 28]
Same lecturer for both. Paxos lecture based on and improved from exist- videos
ing materials used in several universities. Paxos lecture is 14% longer.
Equal quiz difficulty Questions grouped in difficulty and paired across exams.
quizzes
Fair grading
Used rubric. Graded in random order, alternating between quizzes.
rubric
Table 1: Concerns of possible bias against Paxos in the study, steps taken to counter each, and additional materials available.

316 2014 USENIX Annual Technical Conference

USENIX Association

cumulative percent

100%

average to elect a leader (the longest trial took 152ms).
However, lowering the timeouts beyond this point violates
Raft’s timing requirement: leaders have difficulty broadcasting heartbeats before other servers start new elections.
This can cause unnecessary leader changes and lower
overall system availability. We recommend using a conservative election timeout such as 150–300ms; such timeouts are unlikely to cause unnecessary leader changes and
will still provide good availability.

80%
60%
150-150ms
150-151ms
150-155ms
150-175ms
150-200ms
150-300ms

40%
20%
0%
100

1000

10000

100000

cumulative percent

100%
80%

9 Related work

60%
12-24ms
25-50ms
50-100ms
100-200ms
150-300ms

40%
20%
0%
0

100

200
300
400
time without leader (ms)

500

600

Figure 14: The time to detect and replace a crashed leader.
The top graph varies the amount of randomness in election
timeouts, and the bottom graph scales the minimum election
timeout. Each line represents 1000 trials (except for 100 trials for “150–150ms”) and corresponds to a particular choice
of election timeouts; for example, “150–155ms” means that
election timeouts were chosen randomly and uniformly between 150ms and 155ms. The measurements were taken on a
cluster of five servers with a broadcast time of roughly 15ms.
Results for a cluster of nine servers are similar.

quickly? Second, what is the minimum downtime that can
be achieved after leader crashes?
To measure leader election, we repeatedly crashed the
leader of a cluster of five servers and timed how long it
took to detect the crash and elect a new leader (see Figure 14). To generate a worst-case scenario, the servers in
each trial had different log lengths, so some candidates
were not eligible to become leader. Furthermore, to encourage split votes, our test script triggered a synchronized broadcast of heartbeat RPCs from the leader before
terminating its process (this approximates the behavior
of the leader replicating a new log entry prior to crashing). The leader was crashed uniformly randomly within
its heartbeat interval, which was half of the minimum
election timeout for all tests. Thus, the smallest possible
downtime was about half of the minimum election timeout.
The top graph in Figure 14 shows that a small amount
of randomization in the election timeout is enough to
avoid split votes in elections. In the absence of randomness, leader election consistently took longer than 10 seconds in our tests due to many split votes. Adding just 5ms
of randomness helps significantly, resulting in a median
downtime of 287ms. Using more randomness improves
worst-case behavior: with 50ms of randomness the worstcase completion time (over 1000 trials) was 513ms.
The bottom graph in Figure 14 shows that downtime
can be reduced by reducing the election timeout. With
an election timeout of 12–24ms, it takes only 35ms on

USENIX Association

There have been numerous publications related to consensus algorithms, many of which fall into one of the following categories:
• Lamport’s original description of Paxos [13], and attempts to explain it more clearly [14, 18, 19].
• Elaborations of Paxos, which fill in missing details and
modify the algorithm to provide a better foundation for
implementation [24, 35, 11].
• Systems that implement consensus algorithms, such as
Chubby [2, 4], ZooKeeper [9, 10], and Spanner [5]. The
algorithms for Chubby and Spanner have not been published in detail, though both claim to be based on Paxos.
ZooKeeper’s algorithm has been published in more detail, but it is quite different from Paxos.
• Performance optimizations that can be applied to
Paxos [16, 17, 3, 23, 1, 25].
• Oki and Liskov’s Viewstamped Replication (VR), an
alternative approach to consensus developed around the
same time as Paxos. The original description [27] was
intertwined with a protocol for distributed transactions,
but the core consensus protocol has been separated in
a recent update [20]. VR uses a leader-based approach
with many similarities to Raft.
The greatest difference between Raft and Paxos is
Raft’s strong leadership: Raft uses leader election as an
essential part of the consensus protocol, and it concentrates as much functionality as possible in the leader. This
approach results in a simpler algorithm that is easier to
understand. For example, in Paxos, leader election is orthogonal to the basic consensus protocol: it serves only as
a performance optimization and is not required for achieving consensus. However, this results in additional mechanism: Paxos includes both a two-phase protocol for basic
consensus and a separate mechanism for leader election.
In contrast, Raft incorporates leader election directly into
the consensus algorithm and uses it as the first of the two
phases of consensus. This results in less mechanism than
in Paxos.
Like Raft, VR and ZooKeeper are leader-based and
therefore share many of Raft’s advantages over Paxos.
However, Raft has less mechanism that VR or ZooKeeper
because it minimizes the functionality in non-leaders. For
example, log entries in Raft flow in only one direction:
outward from the leader in AppendEntries RPCs. In VR

2014 USENIX Annual Technical Conference 317

log entries flow in both directions (leaders can receive
log entries during the election process); this results in
additional mechanism and complexity. The published description of ZooKeeper also transfers log entries both to
and from the leader, but the implementation is apparently
more like Raft [32].
Raft has fewer message types than any other algorithm
for consensus-based log replication that we are aware of.
For example, VR and ZooKeeper each define 10 different message types, while Raft has only 4 message types
(two RPC requests and their responses). Raft’s messages
are a bit more dense than the other algorithms’, but they
are simpler collectively. In addition, VR and ZooKeeper
are described in terms of transmitting entire logs during
leader changes; additional message types will be required
to optimize these mechanisms so that they are practical.
Several different approaches for cluster membership changes have been proposed or implemented in
other work, including Lamport’s original proposal [13],
VR [20], and SMART [22]. We chose the joint consensus
approach for Raft because it leverages the rest of the consensus protocol, so that very little additional mechanism
is required for membership changes. Lamport’s α -based
approach was not an option for Raft because it assumes
consensus can be reached without a leader. In comparison
to VR and SMART, Raft’s reconfiguration algorithm has
the advantage that membership changes can occur without limiting the processing of normal requests; in contrast, VR stops all normal processing during configuration changes, and SMART imposes an α -like limit on the
number of outstanding requests. Raft’s approach also adds
less mechanism than either VR or SMART.

10 Conclusion
Algorithms are often designed with correctness, efficiency, and/or conciseness as the primary goals. Although
these are all worthy goals, we believe that understandability is just as important. None of the other goals can be
achieved until developers render the algorithm into a practical implementation, which will inevitably deviate from
and expand upon the published form. Unless developers
have a deep understanding of the algorithm and can create intuitions about it, it will be difficult for them to retain
its desirable properties in their implementation.
In this paper we addressed the issue of distributed consensus, where a widely accepted but impenetrable algorithm, Paxos, has challenged students and developers for
many years. We developed a new algorithm, Raft, which
we have shown to be more understandable than Paxos.
We also believe that Raft provides a better foundation
for system building. Using understandability as the primary design goal changed the way we approached the design of Raft; as the design progressed we found ourselves
reusing a few techniques repeatedly, such as decomposing
the problem and simplifying the state space. These tech-

318 2014 USENIX Annual Technical Conference

niques not only improved the understandability of Raft
but also made it easier to convince ourselves of its correctness.

