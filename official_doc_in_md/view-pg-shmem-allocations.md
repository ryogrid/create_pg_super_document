52.26. `pg_shmem_allocations`  
---  
[Prev](view-pg-shadow.md "52.25. pg_shadow") | [Up](views.md "Chapter 52. System Views")| Chapter 52. System Views| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](view-pg-stats.md "52.27. pg_stats")  
  
* * *

## 52.26. `pg_shmem_allocations` #

The `pg_shmem_allocations` view shows allocations made from the server's main shared memory segment. This includes both memory allocated by PostgreSQL itself and memory allocated by extensions using the mechanisms detailed in [Section 36.10.10](xfunc-c.md#XFUNC-SHARED-ADDIN "36.10.10. Shared Memory"). 

Note that this view does not include memory allocated using the dynamic shared memory infrastructure. 

**Table 52.26.`pg_shmem_allocations` Columns**

Column Type  Description   
---  
`name` `text` The name of the shared memory allocation. NULL for unused memory and `<anonymous>` for anonymous allocations.   
`off` `int8` The offset at which the allocation starts. NULL for anonymous allocations, since details related to them are not known.   
`size` `int8` Size of the allocation in bytes   
`allocated_size` `int8` Size of the allocation in bytes including padding. For anonymous allocations, no information about padding is available, so the `size` and `allocated_size` columns will always be equal. Padding is not meaningful for free memory, so the columns will be equal in that case also.   
  
  


Anonymous allocations are allocations that have been made with `ShmemAlloc()` directly, rather than via `ShmemInitStruct()` or `ShmemInitHash()`. 

By default, the `pg_shmem_allocations` view can be read only by superusers or roles with privileges of the `pg_read_all_stats` role. 

* * *

[Prev](view-pg-shadow.md "52.25. pg_shadow") | [Up](views.md "Chapter 52. System Views")|  [Next](view-pg-stats.md "52.27. pg_stats")  
---|---|---  
52.25. `pg_shadow` | [Home](index.md "PostgreSQL 17.5 Documentation")|  52.27. `pg_stats`
