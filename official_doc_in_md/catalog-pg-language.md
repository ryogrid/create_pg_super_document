51.29. `pg_language`  
---  
[Prev](catalog-pg-init-privs.md "51.28. pg_init_privs") | [Up](catalogs.md "Chapter 51. System Catalogs")| Chapter 51. System Catalogs| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](catalog-pg-largeobject.md "51.30. pg_largeobject")  
  
* * *

## 51.29. `pg_language` #

The catalog `pg_language` registers languages in which you can write functions or stored procedures. See [CREATE LANGUAGE](sql-createlanguage.md "CREATE LANGUAGE") and [Chapter 40](xplang.md "Chapter 40. Procedural Languages") for more information about language handlers. 

**Table 51.29.`pg_language` Columns**

Column Type  Description   
---  
`oid` `oid` Row identifier   
`lanname` `name` Name of the language   
`lanowner` `oid` (references [`pg_authid`](catalog-pg-authid.md "51.8. pg_authid").`oid`)  Owner of the language   
`lanispl` `bool` This is false for internal languages (such as SQL) and true for user-defined languages. Currently, pg_dump still uses this to determine which languages need to be dumped, but this might be replaced by a different mechanism in the future.   
`lanpltrusted` `bool` True if this is a trusted language, which means that it is believed not to grant access to anything outside the normal SQL execution environment. Only superusers can create functions in untrusted languages.   
`lanplcallfoid` `oid` (references [`pg_proc`](catalog-pg-proc.md "51.39. pg_proc").`oid`)  For noninternal languages this references the language handler, which is a special function that is responsible for executing all functions that are written in the particular language. Zero for internal languages.   
`laninline` `oid` (references [`pg_proc`](catalog-pg-proc.md "51.39. pg_proc").`oid`)  This references a function that is responsible for executing “inline” anonymous code blocks ([DO](sql-do.md "DO") blocks). Zero if inline blocks are not supported.   
`lanvalidator` `oid` (references [`pg_proc`](catalog-pg-proc.md "51.39. pg_proc").`oid`)  This references a language validator function that is responsible for checking the syntax and validity of new functions when they are created. Zero if no validator is provided.   
`lanacl` `aclitem[]` Access privileges; see [Section 5.8](ddl-priv.md "5.8. Privileges") for details   
  
  


* * *

[Prev](catalog-pg-init-privs.md "51.28. pg_init_privs") | [Up](catalogs.md "Chapter 51. System Catalogs")|  [Next](catalog-pg-largeobject.md "51.30. pg_largeobject")  
---|---|---  
51.28. `pg_init_privs` | [Home](index.md "PostgreSQL 17.5 Documentation")|  51.30. `pg_largeobject`
