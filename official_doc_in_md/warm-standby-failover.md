26.3. Failover  
---  
[Prev](warm-standby.md "26.2. Log-Shipping Standby Servers") | [Up](high-availability.md "Chapter 26. High Availability, Load Balancing, and Replication")| Chapter 26. High Availability, Load Balancing, and Replication| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](hot-standby.md "26.4. Hot Standby")  
  
* * *

## 26.3. Failover #

If the primary server fails then the standby server should begin failover procedures. 

If the standby server fails then no failover need take place. If the standby server can be restarted, even some time later, then the recovery process can also be restarted immediately, taking advantage of restartable recovery. If the standby server cannot be restarted, then a full new standby server instance should be created. 

If the primary server fails and the standby server becomes the new primary, and then the old primary restarts, you must have a mechanism for informing the old primary that it is no longer the primary. This is sometimes known as STONITH (Shoot The Other Node In The Head), which is necessary to avoid situations where both systems think they are the primary, which will lead to confusion and ultimately data loss. 

Many failover systems use just two systems, the primary and the standby, connected by some kind of heartbeat mechanism to continually verify the connectivity between the two and the viability of the primary. It is also possible to use a third system (called a witness server) to prevent some cases of inappropriate failover, but the additional complexity might not be worthwhile unless it is set up with sufficient care and rigorous testing. 

PostgreSQL does not provide the system software required to identify a failure on the primary and notify the standby database server. Many such tools exist and are well integrated with the operating system facilities required for successful failover, such as IP address migration. 

Once failover to the standby occurs, there is only a single server in operation. This is known as a degenerate state. The former standby is now the primary, but the former primary is down and might stay down. To return to normal operation, a standby server must be recreated, either on the former primary system when it comes up, or on a third, possibly new, system. The [pg_rewind](app-pgrewind.md "pg_rewind") utility can be used to speed up this process on large clusters. Once complete, the primary and standby can be considered to have switched roles. Some people choose to use a third server to provide backup for the new primary until the new standby server is recreated, though clearly this complicates the system configuration and operational processes. 

So, switching from primary to standby server can be fast but requires some time to re-prepare the failover cluster. Regular switching from primary to standby is useful, since it allows regular downtime on each system for maintenance. This also serves as a test of the failover mechanism to ensure that it will really work when you need it. Written administration procedures are advised. 

If you have opted for logical replication slot synchronization (see [Section 47.2.3](logicaldecoding-explanation.md#LOGICALDECODING-REPLICATION-SLOTS-SYNCHRONIZATION "47.2.3. Replication Slot Synchronization")), then before switching to the standby server, it is recommended to check if the logical slots synchronized on the standby server are ready for failover. This can be done by following the steps described in [Section 29.3](logical-replication-failover.md "29.3. Logical Replication Failover"). 

To trigger failover of a log-shipping standby server, run `pg_ctl promote` or call `pg_promote()`. If you're setting up reporting servers that are only used to offload read-only queries from the primary, not for high availability purposes, you don't need to promote. 

* * *

[Prev](warm-standby.md "26.2. Log-Shipping Standby Servers") | [Up](high-availability.md "Chapter 26. High Availability, Load Balancing, and Replication")|  [Next](hot-standby.md "26.4. Hot Standby")  
---|---|---  
26.2. Log-Shipping Standby Servers | [Home](index.md "PostgreSQL 17.5 Documentation")|  26.4. Hot Standby
