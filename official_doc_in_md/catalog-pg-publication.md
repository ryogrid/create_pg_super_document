51.40. `pg_publication`  
---  
[Prev](catalog-pg-proc.md "51.39. pg_proc") | [Up](catalogs.md "Chapter 51. System Catalogs")| Chapter 51. System Catalogs| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](catalog-pg-publication-namespace.md "51.41. pg_publication_namespace")  
  
* * *

## 51.40. `pg_publication` #

The catalog `pg_publication` contains all publications created in the database. For more on publications see [Section 29.1](logical-replication-publication.md "29.1. Publication"). 

**Table 51.40.`pg_publication` Columns**

Column Type  Description   
---  
`oid` `oid` Row identifier   
`pubname` `name` Name of the publication   
`pubowner` `oid` (references [`pg_authid`](catalog-pg-authid.md "51.8. pg_authid").`oid`)  Owner of the publication   
`puballtables` `bool` If true, this publication automatically includes all tables in the database, including any that will be created in the future.   
`pubinsert` `bool` If true, [INSERT](sql-insert.md "INSERT") operations are replicated for tables in the publication.   
`pubupdate` `bool` If true, [UPDATE](sql-update.md "UPDATE") operations are replicated for tables in the publication.   
`pubdelete` `bool` If true, [DELETE](sql-delete.md "DELETE") operations are replicated for tables in the publication.   
`pubtruncate` `bool` If true, [TRUNCATE](sql-truncate.md "TRUNCATE") operations are replicated for tables in the publication.   
`pubviaroot` `bool` If true, operations on a leaf partition are replicated using the identity and schema of its topmost partitioned ancestor mentioned in the publication instead of its own.   
  
  


* * *

[Prev](catalog-pg-proc.md "51.39. pg_proc") | [Up](catalogs.md "Chapter 51. System Catalogs")|  [Next](catalog-pg-publication-namespace.md "51.41. pg_publication_namespace")  
---|---|---  
51.39. `pg_proc` | [Home](index.md "PostgreSQL 17.5 Documentation")|  51.41. `pg_publication_namespace`
