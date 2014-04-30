*Note: The project is under development. It is not ready for deployment yet.*

Barista
====
Barista is a distributed, synchronously replicated, fault tolerant relational data store. It runs as a middleware service over database instances to provide an abstraction for a distributed relational store. It ensures that the data is replicated across many sets of Paxos state machines in replica groups to provide fault-tolerance and recovery. The replication enables load balancing and availability; clients automatically failover between replicas. Barista exposes SQL for data management. Client applications can use Barista with the same SQL code they used before, and under the hood it guarantees replication, consistency, and fault-tolerance seamlessly.
