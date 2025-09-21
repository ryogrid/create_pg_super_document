51.49. `pg_shdescription`  
---  
[Prev](catalog-pg-shdepend.md "51.48. pg_shdepend") | [Up](catalogs.md "Chapter 51. System Catalogs")| Chapter 51. System Catalogs| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](catalog-pg-shseclabel.md "51.50. pg_shseclabel")  
  
* * *

## 51.49. `pg_shdescription` #

The catalog `pg_shdescription` stores optional descriptions (comments) for shared database objects. Descriptions can be manipulated with the [`COMMENT`](sql-comment.md "COMMENT") command and viewed with psql's `\d` commands. 

See also [`pg_description`](catalog-pg-description.md "51.19. pg_description"), which performs a similar function for descriptions involving objects within a single database. 

Unlike most system catalogs, `pg_shdescription` is shared across all databases of a cluster: there is only one copy of `pg_shdescription` per cluster, not one per database. 

**Table 51.49.`pg_shdescription` Columns**

Column Type  Description   
---  
`objoid` `oid` (references any OID column)  The OID of the object this description pertains to   
`classoid` `oid` (references [`pg_class`](catalog-pg-class.md "51.11. pg_class").`oid`)  The OID of the system catalog this object appears in   
`description` `text` Arbitrary text that serves as the description of this object   
  
  


* * *

[Prev](catalog-pg-shdepend.md "51.48. pg_shdepend") | [Up](catalogs.md "Chapter 51. System Catalogs")|  [Next](catalog-pg-shseclabel.md "51.50. pg_shseclabel")  
---|---|---  
51.48. `pg_shdepend` | [Home](index.md "PostgreSQL 17.5 Documentation")|  51.50. `pg_shseclabel`
