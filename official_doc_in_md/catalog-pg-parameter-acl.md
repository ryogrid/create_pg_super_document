51.36. `pg_parameter_acl`  
---  
[Prev](catalog-pg-opfamily.md "51.35. pg_opfamily") | [Up](catalogs.md "Chapter 51. System Catalogs")| Chapter 51. System Catalogs| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](catalog-pg-partitioned-table.md "51.37. pg_partitioned_table")  
  
* * *

## 51.36. `pg_parameter_acl` #

The catalog `pg_parameter_acl` records configuration parameters for which privileges have been granted to one or more roles. No entry is made for parameters that have default privileges. 

Unlike most system catalogs, `pg_parameter_acl` is shared across all databases of a cluster: there is only one copy of `pg_parameter_acl` per cluster, not one per database. 

**Table 51.36.`pg_parameter_acl` Columns**

Column Type  Description   
---  
`oid` `oid` Row identifier   
`parname` `text` The name of a configuration parameter for which privileges are granted   
`paracl` `aclitem[]` Access privileges; see [Section 5.8](ddl-priv.md "5.8. Privileges") for details   
  
  


* * *

[Prev](catalog-pg-opfamily.md "51.35. pg_opfamily") | [Up](catalogs.md "Chapter 51. System Catalogs")|  [Next](catalog-pg-partitioned-table.md "51.37. pg_partitioned_table")  
---|---|---  
51.35. `pg_opfamily` | [Home](index.md "PostgreSQL 17.5 Documentation")|  51.37. `pg_partitioned_table`
