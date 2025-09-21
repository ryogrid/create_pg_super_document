52.4. `pg_backend_memory_contexts`  
---  
[Prev](view-pg-available-extension-versions.md "52.3. pg_available_extension_versions") | [Up](views.md "Chapter 52. System Views")| Chapter 52. System Views| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](view-pg-config.md "52.5. pg_config")  
  
* * *

## 52.4. `pg_backend_memory_contexts` #

The view `pg_backend_memory_contexts` displays all the memory contexts of the server process attached to the current session. 

`pg_backend_memory_contexts` contains one row for each memory context. 

**Table 52.4.`pg_backend_memory_contexts` Columns**

Column Type  Description   
---  
`name` `text` Name of the memory context   
`ident` `text` Identification information of the memory context. This field is truncated at 1024 bytes   
`parent` `text` Name of the parent of this memory context   
`level` `int4` Distance from TopMemoryContext in context tree   
`total_bytes` `int8` Total bytes allocated for this memory context   
`total_nblocks` `int8` Total number of blocks allocated for this memory context   
`free_bytes` `int8` Free space in bytes   
`free_chunks` `int8` Total number of free chunks   
`used_bytes` `int8` Used space in bytes   
  
  


By default, the `pg_backend_memory_contexts` view can be read only by superusers or roles with the privileges of the `pg_read_all_stats` role. 

* * *

[Prev](view-pg-available-extension-versions.md "52.3. pg_available_extension_versions") | [Up](views.md "Chapter 52. System Views")|  [Next](view-pg-config.md "52.5. pg_config")  
---|---|---  
52.3. `pg_available_extension_versions` | [Home](index.md "PostgreSQL 17.5 Documentation")|  52.5. `pg_config`
