52.5. `pg_config`  
---  
[Prev](view-pg-backend-memory-contexts.md "52.4. pg_backend_memory_contexts") | [Up](views.md "Chapter 52. System Views")| Chapter 52. System Views| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](view-pg-cursors.md "52.6. pg_cursors")  
  
* * *

## 52.5. `pg_config` #

The view `pg_config` describes the compile-time configuration parameters of the currently installed version of PostgreSQL. It is intended, for example, to be used by software packages that want to interface to PostgreSQL to facilitate finding the required header files and libraries. It provides the same basic information as the [pg_config](app-pgconfig.md "pg_config") PostgreSQL client application. 

By default, the `pg_config` view can be read only by superusers. 

**Table 52.5.`pg_config` Columns**

Column Type  Description   
---  
`name` `text` The parameter name   
`setting` `text` The parameter value   
  
  


* * *

[Prev](view-pg-backend-memory-contexts.md "52.4. pg_backend_memory_contexts") | [Up](views.md "Chapter 52. System Views")|  [Next](view-pg-cursors.md "52.6. pg_cursors")  
---|---|---  
52.4. `pg_backend_memory_contexts` | [Home](index.md "PostgreSQL 17.5 Documentation")|  52.6. `pg_cursors`
