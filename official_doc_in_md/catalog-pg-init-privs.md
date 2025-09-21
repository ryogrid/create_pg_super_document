51.28. `pg_init_privs`  
---  
[Prev](catalog-pg-inherits.md "51.27. pg_inherits") | [Up](catalogs.md "Chapter 51. System Catalogs")| Chapter 51. System Catalogs| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](catalog-pg-language.md "51.29. pg_language")  
  
* * *

## 51.28. `pg_init_privs` #

The catalog `pg_init_privs` records information about the initial privileges of objects in the system. There is one entry for each object in the database which has a non-default (non-NULL) initial set of privileges. 

Objects can have initial privileges either by having those privileges set when the system is initialized (by initdb) or when the object is created during a [`CREATE EXTENSION`](sql-createextension.md "CREATE EXTENSION") and the extension script sets initial privileges using the [`GRANT`](sql-grant.md "GRANT") system. Note that the system will automatically handle recording of the privileges during the extension script and that extension authors need only use the `GRANT` and `REVOKE` statements in their script to have the privileges recorded. The `privtype` column indicates if the initial privilege was set by initdb or during a `CREATE EXTENSION` command. 

Objects which have initial privileges set by initdb will have entries where `privtype` is `'i'`, while objects which have initial privileges set by `CREATE EXTENSION` will have entries where `privtype` is `'e'`. 

**Table 51.28.`pg_init_privs` Columns**

Column Type  Description   
---  
`objoid` `oid` (references any OID column)  The OID of the specific object   
`classoid` `oid` (references [`pg_class`](catalog-pg-class.md "51.11. pg_class").`oid`)  The OID of the system catalog the object is in   
`objsubid` `int4` For a table column, this is the column number (the `objoid` and `classoid` refer to the table itself). For all other object types, this column is zero.   
`privtype` `char` A code defining the type of initial privilege of this object; see text   
`initprivs` `aclitem[]` The initial access privileges; see [Section 5.8](ddl-priv.md "5.8. Privileges") for details   
  
  


* * *

[Prev](catalog-pg-inherits.md "51.27. pg_inherits") | [Up](catalogs.md "Chapter 51. System Catalogs")|  [Next](catalog-pg-language.md "51.29. pg_language")  
---|---|---  
51.27. `pg_inherits` | [Home](index.md "PostgreSQL 17.5 Documentation")|  51.29. `pg_language`
