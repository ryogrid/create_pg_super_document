51.44. `pg_replication_origin`  
---  
[Prev](catalog-pg-range.md "51.43. pg_range") | [Up](catalogs.md "Chapter 51. System Catalogs")| Chapter 51. System Catalogs| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](catalog-pg-rewrite.md "51.45. pg_rewrite")  
  
* * *

## 51.44. `pg_replication_origin` #

The `pg_replication_origin` catalog contains all replication origins created. For more on replication origins see [Chapter 48](replication-origins.md "Chapter 48. Replication Progress Tracking"). 

Unlike most system catalogs, `pg_replication_origin` is shared across all databases of a cluster: there is only one copy of `pg_replication_origin` per cluster, not one per database. 

**Table 51.44.`pg_replication_origin` Columns**

Column Type  Description   
---  
`roident` `oid` A unique, cluster-wide identifier for the replication origin. Should never leave the system.   
`roname` `text` The external, user defined, name of a replication origin.   
  
  


* * *

[Prev](catalog-pg-range.md "51.43. pg_range") | [Up](catalogs.md "Chapter 51. System Catalogs")|  [Next](catalog-pg-rewrite.md "51.45. pg_rewrite")  
---|---|---  
51.43. `pg_range` | [Home](index.md "PostgreSQL 17.5 Documentation")|  51.45. `pg_rewrite`
