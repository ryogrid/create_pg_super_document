51.19. `pg_description`  
---  
[Prev](catalog-pg-depend.md "51.18. pg_depend") | [Up](catalogs.md "Chapter 51. System Catalogs")| Chapter 51. System Catalogs| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](catalog-pg-enum.md "51.20. pg_enum")  
  
* * *

## 51.19. `pg_description` #

The catalog `pg_description` stores optional descriptions (comments) for each database object. Descriptions can be manipulated with the [`COMMENT`](sql-comment.md "COMMENT") command and viewed with psql's `\d` commands. Descriptions of many built-in system objects are provided in the initial contents of `pg_description`. 

See also [`pg_shdescription`](catalog-pg-shdescription.md "51.49. pg_shdescription"), which performs a similar function for descriptions involving objects that are shared across a database cluster. 

**Table 51.19.`pg_description` Columns**

Column Type  Description   
---  
`objoid` `oid` (references any OID column)  The OID of the object this description pertains to   
`classoid` `oid` (references [`pg_class`](catalog-pg-class.md "51.11. pg_class").`oid`)  The OID of the system catalog this object appears in   
`objsubid` `int4` For a comment on a table column, this is the column number (the `objoid` and `classoid` refer to the table itself). For all other object types, this column is zero.   
`description` `text` Arbitrary text that serves as the description of this object   
  
  


* * *

[Prev](catalog-pg-depend.md "51.18. pg_depend") | [Up](catalogs.md "Chapter 51. System Catalogs")|  [Next](catalog-pg-enum.md "51.20. pg_enum")  
---|---|---  
51.18. `pg_depend` | [Home](index.md "PostgreSQL 17.5 Documentation")|  51.20. `pg_enum`
