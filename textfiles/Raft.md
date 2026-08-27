### Leader Election: A Raft-Based Consensus Approach

This GFS implementation uses a Raft-like consensus algorithm to ensure high availability of the master node. If the current leader (primary master) fails, the remaining masters can automatically elect a new one without manual intervention, ensuring the file system remains operational.

#### Server States

Every master node in the cluster is in one of three states at any given time:

* `Follower`: The default, passive state. Followers respond to requests from the Leader and Candidates.
* `Candidate`: A node that has started an election to become the new leader.
* `Leader`: The node responsible for handling all client interactions and managing metadata replication. There can only be one leader per term.

#### Core Concepts

The election process is governed by these key concepts:

* **Terms**: Time in the cluster is divided into sequential, numbered **terms**. Each term begins with an election. Terms act as a logical clock, allowing servers to identify and discard outdated information or messages from previous leaders.
* **Election Timeout**: Each `Follower` maintains a randomized timeout (e.g., 150-300ms). If a Follower does not receive a message (a "heartbeat") from the current Leader before this timer expires, it assumes the Leader has failed and starts a new election. The randomness is crucial for preventing multiple nodes from starting an election simultaneously, which could lead to a split vote.
* **Heartbeats**: The `Leader` sends periodic, small messages to all `Followers` to assert its authority. Upon receiving a heartbeat, a Follower knows the Leader is still active and resets its own election timer.

#### The Election Process

1.  **Leader Failure Detection**: A `Follower`'s election timer expires because it has not received a heartbeat from the current `Leader`.
2.  **Transition to Candidate**: The `Follower` transitions to the `Candidate` state and immediately performs the following actions:
    * Increments the current term number.
    * Votes for itself.
    * Sends a `RequestVote` message to all other master nodes in the cluster.
3.  **Voting**: Upon receiving a `RequestVote` message, other `Follower` nodes will grant their vote under two conditions:
    * They have not already voted for another candidate in the current term.
    * The candidate's log of operations is at least as up-to-date as their own.
4.  **Election Outcome**: The election can have one of three outcomes:
    * **Success**: If the `Candidate` receives votes from a **majority** of the servers, it is promoted to `Leader`. It then immediately begins sending heartbeats to all other servers to establish its leadership and prevent new elections.
    * **Split Vote**: If multiple nodes become candidates simultaneously, the votes may be split so that no candidate achieves a majority. In this case, the term ends without a leader. The nodes' election timers will expire again, and a new election will begin for the next term. The randomized timers make repeated split votes highly unlikely.
    * **Discovery of an Existing Leader**: If a `Candidate` receives a heartbeat from another node claiming to be the leader (with a term number equal to or greater than its own), it recognizes the other node's authority, immediately steps down, and returns to the `Follower` state.