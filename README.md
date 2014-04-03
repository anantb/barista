*Note: The project is under development. It is not ready for deployment yet.*

Barista
====
Barista is a distributed, synchronously replicated, fault tolerant, relational data store. It is a layer written over PostgreSQL that manages realtime replication of data in a distributed infrastructure to provide fault-tolerance and load balancing. All writes are propagated synchronously using paxos. Barista exposes SQL for data management.
 
Client applications can use the same SQL with Barista and under the hood it takes care of load balancing, consistency, and fault-tolerance seamlessly.
