51.23. `pg_foreign_data_wrapper`  
---  
[Prev](catalog-pg-extension.md "51.22. pg_extension") | [Up](catalogs.md "Chapter 51. System Catalogs")| Chapter 51. System Catalogs| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](catalog-pg-foreign-server.md "51.24. pg_foreign_server")  
  
* * *

## 51.23. `pg_foreign_data_wrapper` #

The catalog `pg_foreign_data_wrapper` stores foreign-data wrapper definitions. A foreign-data wrapper is the mechanism by which external data, residing on foreign servers, is accessed. 

**Table 51.23.`pg_foreign_data_wrapper` Columns**

Column Type  Description   
---  
`oid` `oid` Row identifier   
`fdwname` `name` Name of the foreign-data wrapper   
`fdwowner` `oid` (references [`pg_authid`](catalog-pg-authid.md "51.8. pg_authid").`oid`)  Owner of the foreign-data wrapper   
`fdwhandler` `oid` (references [`pg_proc`](catalog-pg-proc.md "51.39. pg_proc").`oid`)  References a handler function that is responsible for supplying execution routines for the foreign-data wrapper. Zero if no handler is provided   
`fdwvalidator` `oid` (references [`pg_proc`](catalog-pg-proc.md "51.39. pg_proc").`oid`)  References a validator function that is responsible for checking the validity of the options given to the foreign-data wrapper, as well as options for foreign servers and user mappings using the foreign-data wrapper. Zero if no validator is provided   
`fdwacl` `aclitem[]` Access privileges; see [Section 5.8](ddl-priv.md "5.8. Privileges") for details   
`fdwoptions` `text[]` Foreign-data wrapper specific options, as “keyword=value” strings   
  
  


* * *

[Prev](catalog-pg-extension.md "51.22. pg_extension") | [Up](catalogs.md "Chapter 51. System Catalogs")|  [Next](catalog-pg-foreign-server.md "51.24. pg_foreign_server")  
---|---|---  
51.22. `pg_extension` | [Home](index.md "PostgreSQL 17.5 Documentation")|  51.24. `pg_foreign_server`
