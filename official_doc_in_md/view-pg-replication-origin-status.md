52.18. `pg_replication_origin_status`  
---  
[Prev](view-pg-publication-tables.md "52.17. pg_publication_tables") | [Up](views.md "Chapter 52. System Views")| Chapter 52. System Views| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](view-pg-replication-slots.md "52.19. pg_replication_slots")  
  
* * *

## 52.18. `pg_replication_origin_status` #

The `pg_replication_origin_status` view contains information about how far replay for a certain origin has progressed. For more on replication origins see [Chapter 48](replication-origins.md "Chapter 48. Replication Progress Tracking"). 

**Table 52.18.`pg_replication_origin_status` Columns**

Column Type  Description   
---  
`local_id` `oid` (references [`pg_replication_origin`](catalog-pg-replication-origin.md "51.44. pg_replication_origin").`roident`)  internal node identifier   
`external_id` `text` (references [`pg_replication_origin`](catalog-pg-replication-origin.md "51.44. pg_replication_origin").`roname`)  external node identifier   
`remote_lsn` `pg_lsn` The origin node's LSN up to which data has been replicated.   
`local_lsn` `pg_lsn` This node's LSN at which `remote_lsn` has been replicated. Used to flush commit records before persisting data to disk when using asynchronous commits.   
  
  


* * *

[Prev](view-pg-publication-tables.md "52.17. pg_publication_tables") | [Up](views.md "Chapter 52. System Views")|  [Next](view-pg-replication-slots.md "52.19. pg_replication_slots")  
---|---|---  
52.17. `pg_publication_tables` | [Home](index.md "PostgreSQL 17.5 Documentation")|  52.19. `pg_replication_slots`
