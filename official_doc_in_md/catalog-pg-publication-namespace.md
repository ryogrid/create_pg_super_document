51.41. `pg_publication_namespace`  
---  
[Prev](catalog-pg-publication.md "51.40. pg_publication") | [Up](catalogs.md "Chapter 51. System Catalogs")| Chapter 51. System Catalogs| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](catalog-pg-publication-rel.md "51.42. pg_publication_rel")  
  
* * *

## 51.41. `pg_publication_namespace` #

The catalog `pg_publication_namespace` contains the mapping between schemas and publications in the database. This is a many-to-many mapping. 

**Table 51.41.`pg_publication_namespace` Columns**

Column Type  Description   
---  
`oid` `oid` Row identifier   
`pnpubid` `oid` (references [`pg_publication`](catalog-pg-publication.md "51.40. pg_publication").`oid`)  Reference to publication   
`pnnspid` `oid` (references [`pg_namespace`](catalog-pg-namespace.md "51.32. pg_namespace").`oid`)  Reference to schema   
  
  


* * *

[Prev](catalog-pg-publication.md "51.40. pg_publication") | [Up](catalogs.md "Chapter 51. System Catalogs")|  [Next](catalog-pg-publication-rel.md "51.42. pg_publication_rel")  
---|---|---  
51.40. `pg_publication` | [Home](index.md "PostgreSQL 17.5 Documentation")|  51.42. `pg_publication_rel`
