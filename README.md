Distributed Relational Data Store
====
Distributed Relational Data Store is a virtualization layer written over postgres that replicates the data to multiple machines in a distributed infrastructure in realtime -- all writes are propagated synchronously using paxos to provide fault-tolerance and load balancing. Distributed Relational Data Store exposes SQL for Data Management.
 
Applications will continue to use same SQL and this virtualization layer would ensure load balancing, consistency, and fault-tolerance seamlessly.
