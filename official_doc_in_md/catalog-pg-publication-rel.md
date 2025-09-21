51.42. `pg_publication_rel`  
---  
[Prev](catalog-pg-publication-namespace.md "51.41. pg_publication_namespace") | [Up](catalogs.md "Chapter 51. System Catalogs")| Chapter 51. System Catalogs| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](catalog-pg-range.md "51.43. pg_range")  
  
* * *

## 51.42. `pg_publication_rel` #

The catalog `pg_publication_rel` contains the mapping between relations and publications in the database. This is a many-to-many mapping. See also [Section 52.17](view-pg-publication-tables.md "52.17. pg_publication_tables") for a more user-friendly view of this information. 

**Table 51.42.`pg_publication_rel` Columns**

Column Type  Description   
---  
`oid` `oid` Row identifier   
`prpubid` `oid` (references [`pg_publication`](catalog-pg-publication.md "51.40. pg_publication").`oid`)  Reference to publication   
`prrelid` `oid` (references [`pg_class`](catalog-pg-class.md "51.11. pg_class").`oid`)  Reference to relation   
`prqual` `pg_node_tree` Expression tree (in `nodeToString()` representation) for the relation's publication qualifying condition. Null if there is no publication qualifying condition.  
`prattrs` `int2vector` (references [`pg_attribute`](catalog-pg-attribute.md "51.7. pg_attribute").`attnum`)  This is an array of values that indicates which table columns are part of the publication. For example, a value of `1 3` would mean that the first and the third table columns are published. A null value indicates that all columns are published.   
  
  


* * *

[Prev](catalog-pg-publication-namespace.md "51.41. pg_publication_namespace") | [Up](catalogs.md "Chapter 51. System Catalogs")|  [Next](catalog-pg-range.md "51.43. pg_range")  
---|---|---  
51.41. `pg_publication_namespace` | [Home](index.md "PostgreSQL 17.5 Documentation")|  51.43. `pg_range`
