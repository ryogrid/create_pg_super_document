51.50. `pg_shseclabel`  
---  
[Prev](catalog-pg-shdescription.md "51.49. pg_shdescription") | [Up](catalogs.md "Chapter 51. System Catalogs")| Chapter 51. System Catalogs| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](catalog-pg-statistic.md "51.51. pg_statistic")  
  
* * *

## 51.50. `pg_shseclabel` #

The catalog `pg_shseclabel` stores security labels on shared database objects. Security labels can be manipulated with the [`SECURITY LABEL`](sql-security-label.md "SECURITY LABEL") command. For an easier way to view security labels, see [Section 52.22](view-pg-seclabels.md "52.22. pg_seclabels"). 

See also [`pg_seclabel`](catalog-pg-seclabel.md "51.46. pg_seclabel"), which performs a similar function for security labels involving objects within a single database. 

Unlike most system catalogs, `pg_shseclabel` is shared across all databases of a cluster: there is only one copy of `pg_shseclabel` per cluster, not one per database. 

**Table 51.50.`pg_shseclabel` Columns**

Column Type  Description   
---  
`objoid` `oid` (references any OID column)  The OID of the object this security label pertains to   
`classoid` `oid` (references [`pg_class`](catalog-pg-class.md "51.11. pg_class").`oid`)  The OID of the system catalog this object appears in   
`provider` `text` The label provider associated with this label.   
`label` `text` The security label applied to this object.   
  
  


* * *

[Prev](catalog-pg-shdescription.md "51.49. pg_shdescription") | [Up](catalogs.md "Chapter 51. System Catalogs")|  [Next](catalog-pg-statistic.md "51.51. pg_statistic")  
---|---|---  
51.49. `pg_shdescription` | [Home](index.md "PostgreSQL 17.5 Documentation")|  51.51. `pg_statistic`
