51.46. `pg_seclabel`  
---  
[Prev](catalog-pg-rewrite.md "51.45. pg_rewrite") | [Up](catalogs.md "Chapter 51. System Catalogs")| Chapter 51. System Catalogs| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](catalog-pg-sequence.md "51.47. pg_sequence")  
  
* * *

## 51.46. `pg_seclabel` #

The catalog `pg_seclabel` stores security labels on database objects. Security labels can be manipulated with the [`SECURITY LABEL`](sql-security-label.md "SECURITY LABEL") command. For an easier way to view security labels, see [Section 52.22](view-pg-seclabels.md "52.22. pg_seclabels"). 

See also [`pg_shseclabel`](catalog-pg-shseclabel.md "51.50. pg_shseclabel"), which performs a similar function for security labels of database objects that are shared across a database cluster. 

**Table 51.46.`pg_seclabel` Columns**

Column Type  Description   
---  
`objoid` `oid` (references any OID column)  The OID of the object this security label pertains to   
`classoid` `oid` (references [`pg_class`](catalog-pg-class.md "51.11. pg_class").`oid`)  The OID of the system catalog this object appears in   
`objsubid` `int4` For a security label on a table column, this is the column number (the `objoid` and `classoid` refer to the table itself). For all other object types, this column is zero.   
`provider` `text` The label provider associated with this label.   
`label` `text` The security label applied to this object.   
  
  


* * *

[Prev](catalog-pg-rewrite.md "51.45. pg_rewrite") | [Up](catalogs.md "Chapter 51. System Catalogs")|  [Next](catalog-pg-sequence.md "51.47. pg_sequence")  
---|---|---  
51.45. `pg_rewrite` | [Home](index.md "PostgreSQL 17.5 Documentation")|  51.47. `pg_sequence`
