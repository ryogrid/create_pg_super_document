Chapter 29. Logical Replication  
---  
[Prev](wal-internals.md "28.6. WAL Internals") | [Up](admin.md "Part III. Server Administration")| Part III. Server Administration| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](logical-replication-publication.md "29.1. Publication")  
  
* * *

## Chapter 29. Logical Replication

**Table of Contents**

[29.1. Publication](logical-replication-publication.md)
[29.2. Subscription](logical-replication-subscription.md)
    

[29.2.1. Replication Slot Management](logical-replication-subscription.md#LOGICAL-REPLICATION-SUBSCRIPTION-SLOT)
[29.2.2. Examples: Set Up Logical Replication](logical-replication-subscription.md#LOGICAL-REPLICATION-SUBSCRIPTION-EXAMPLES)
[29.2.3. Examples: Deferred Replication Slot Creation](logical-replication-subscription.md#LOGICAL-REPLICATION-SUBSCRIPTION-EXAMPLES-DEFERRED-SLOT)
[29.3. Logical Replication Failover](logical-replication-failover.md)
[29.4. Row Filters](logical-replication-row-filter.md)
    

[29.4.1. Row Filter Rules](logical-replication-row-filter.md#LOGICAL-REPLICATION-ROW-FILTER-RULES)
[29.4.2. Expression Restrictions](logical-replication-row-filter.md#LOGICAL-REPLICATION-ROW-FILTER-RESTRICTIONS)
[29.4.3. UPDATE Transformations](logical-replication-row-filter.md#LOGICAL-REPLICATION-ROW-FILTER-TRANSFORMATIONS)
[29.4.4. Partitioned Tables](logical-replication-row-filter.md#LOGICAL-REPLICATION-ROW-FILTER-PARTITIONED-TABLE)
[29.4.5. Initial Data Synchronization](logical-replication-row-filter.md#LOGICAL-REPLICATION-ROW-FILTER-INITIAL-DATA-SYNC)
[29.4.6. Combining Multiple Row Filters](logical-replication-row-filter.md#LOGICAL-REPLICATION-ROW-FILTER-COMBINING)
[29.4.7. Examples](logical-replication-row-filter.md#LOGICAL-REPLICATION-ROW-FILTER-EXAMPLES)
[29.5. Column Lists](logical-replication-col-lists.md)
    

[29.5.1. Examples](logical-replication-col-lists.md#LOGICAL-REPLICATION-COL-LIST-EXAMPLES)
[29.6. Conflicts](logical-replication-conflicts.md)
[29.7. Restrictions](logical-replication-restrictions.md)
[29.8. Architecture](logical-replication-architecture.md)
    

[29.8.1. Initial Snapshot](logical-replication-architecture.md#LOGICAL-REPLICATION-SNAPSHOT)
[29.9. Monitoring](logical-replication-monitoring.md)
[29.10. Security](logical-replication-security.md)
[29.11. Configuration Settings](logical-replication-config.md)
    

[29.11.1. Publishers](logical-replication-config.md#LOGICAL-REPLICATION-CONFIG-PUBLISHER)
[29.11.2. Subscribers](logical-replication-config.md#LOGICAL-REPLICATION-CONFIG-SUBSCRIBER)
[29.12. Quick Setup](logical-replication-quick-setup.md)

Logical replication is a method of replicating data objects and their changes, based upon their replication identity (usually a primary key). We use the term logical in contrast to physical replication, which uses exact block addresses and byte-by-byte replication. PostgreSQL supports both mechanisms concurrently, see [Chapter 26](high-availability.md "Chapter 26. High Availability, Load Balancing, and Replication"). Logical replication allows fine-grained control over both data replication and security. 

Logical replication uses a _publish_ and _subscribe_ model with one or more _subscribers_ subscribing to one or more _publications_ on a _publisher_ node. Subscribers pull data from the publications they subscribe to and may subsequently re-publish data to allow cascading replication or more complex configurations. 

Logical replication of a table typically starts with taking a snapshot of the data on the publisher database and copying that to the subscriber. Once that is done, the changes on the publisher are sent to the subscriber as they occur in real-time. The subscriber applies the data in the same order as the publisher so that transactional consistency is guaranteed for publications within a single subscription. This method of data replication is sometimes referred to as transactional replication. 

The typical use-cases for logical replication are: 

  * Sending incremental changes in a single database or a subset of a database to subscribers as they occur. 

  * Firing triggers for individual changes as they arrive on the subscriber. 

  * Consolidating multiple databases into a single one (for example for analytical purposes). 

  * Replicating between different major versions of PostgreSQL. 

  * Replicating between PostgreSQL instances on different platforms (for example Linux to Windows) 

  * Giving access to replicated data to different groups of users. 

  * Sharing a subset of the database between multiple databases. 




The subscriber database behaves in the same way as any other PostgreSQL instance and can be used as a publisher for other databases by defining its own publications. When the subscriber is treated as read-only by application, there will be no conflicts from a single subscription. On the other hand, if there are other writes done either by an application or by other subscribers to the same set of tables, conflicts can arise. 

* * *

[Prev](wal-internals.md "28.6. WAL Internals") | [Up](admin.md "Part III. Server Administration")|  [Next](logical-replication-publication.md "29.1. Publication")  
---|---|---  
28.6. WAL Internals | [Home](index.md "PostgreSQL 17.5 Documentation")|  29.1. Publication
