51.55. `pg_subscription_rel`  
---  
[Prev](catalog-pg-subscription.md "51.54. pg_subscription") | [Up](catalogs.md "Chapter 51. System Catalogs")| Chapter 51. System Catalogs| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](catalog-pg-tablespace.md "51.56. pg_tablespace")  
  
* * *

## 51.55. `pg_subscription_rel` #

The catalog `pg_subscription_rel` contains the state for each replicated relation in each subscription. This is a many-to-many mapping. 

This catalog only contains tables known to the subscription after running either [`CREATE SUBSCRIPTION`](sql-createsubscription.md "CREATE SUBSCRIPTION") or [`ALTER SUBSCRIPTION ... REFRESH PUBLICATION`](sql-altersubscription.md "ALTER SUBSCRIPTION"). 

**Table 51.55.`pg_subscription_rel` Columns**

Column Type  Description   
---  
`srsubid` `oid` (references [`pg_subscription`](catalog-pg-subscription.md "51.54. pg_subscription").`oid`)  Reference to subscription   
`srrelid` `oid` (references [`pg_class`](catalog-pg-class.md "51.11. pg_class").`oid`)  Reference to relation   
`srsubstate` `char` State code: `i` = initialize, `d` = data is being copied, `f` = finished table copy, `s` = synchronized, `r` = ready (normal replication)   
`srsublsn` `pg_lsn` Remote LSN of the state change used for synchronization coordination when in `s` or `r` states, otherwise null   
  
  


* * *

[Prev](catalog-pg-subscription.md "51.54. pg_subscription") | [Up](catalogs.md "Chapter 51. System Catalogs")|  [Next](catalog-pg-tablespace.md "51.56. pg_tablespace")  
---|---|---  
51.54. `pg_subscription` | [Home](index.md "PostgreSQL 17.5 Documentation")|  51.56. `pg_tablespace`
