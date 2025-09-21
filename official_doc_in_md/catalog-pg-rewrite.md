51.45. `pg_rewrite`  
---  
[Prev](catalog-pg-replication-origin.md "51.44. pg_replication_origin") | [Up](catalogs.md "Chapter 51. System Catalogs")| Chapter 51. System Catalogs| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](catalog-pg-seclabel.md "51.46. pg_seclabel")  
  
* * *

## 51.45. `pg_rewrite` #

The catalog `pg_rewrite` stores rewrite rules for tables and views. 

**Table 51.45.`pg_rewrite` Columns**

Column Type  Description   
---  
`oid` `oid` Row identifier   
`rulename` `name` Rule name   
`ev_class` `oid` (references [`pg_class`](catalog-pg-class.md "51.11. pg_class").`oid`)  The table this rule is for   
`ev_type` `char` Event type that the rule is for: 1 = [SELECT](sql-select.md "SELECT"), 2 = [UPDATE](sql-update.md "UPDATE"), 3 = [INSERT](sql-insert.md "INSERT"), 4 = [DELETE](sql-delete.md "DELETE")  
`ev_enabled` `char` Controls in which [session_replication_role](runtime-config-client.md#GUC-SESSION-REPLICATION-ROLE) modes the rule fires. `O` = rule fires in “origin” and “local” modes, `D` = rule is disabled, `R` = rule fires in “replica” mode, `A` = rule fires always.   
`is_instead` `bool` True if the rule is an `INSTEAD` rule   
`ev_qual` `pg_node_tree` Expression tree (in the form of a `nodeToString()` representation) for the rule's qualifying condition   
`ev_action` `pg_node_tree` Query tree (in the form of a `nodeToString()` representation) for the rule's action   
  
  


### Note

`pg_class.relhasrules` must be true if a table has any rules in this catalog. 

* * *

[Prev](catalog-pg-replication-origin.md "51.44. pg_replication_origin") | [Up](catalogs.md "Chapter 51. System Catalogs")|  [Next](catalog-pg-seclabel.md "51.46. pg_seclabel")  
---|---|---  
51.44. `pg_replication_origin` | [Home](index.md "PostgreSQL 17.5 Documentation")|  51.46. `pg_seclabel`
