# TableSpaceOpts

## Location
[src/include/commands/tablespace.h:39-46](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/commands/tablespace.h#L39-L46)

## Overview
A structure that defines configuration options and cost parameters for tablespaces, used to customize I/O behavior and performance characteristics on a per-tablespace basis.

## Definition
```c
typedef struct TableSpaceOpts
{
    int32       vl_len_;                    /* varlena header (do not touch directly!) */
    float8      random_page_cost;
    float8      seq_page_cost;
    int         effective_io_concurrency;
    int         maintenance_io_concurrency;
} TableSpaceOpts;
```

## Detailed Description
The `TableSpaceOpts` structure encapsulates tablespace-specific configuration options that control PostgreSQL's query planner cost estimates and I/O behavior. This structure is used to store reloptions (relation options) for tablespaces, allowing database administrators to fine-tune performance characteristics based on the underlying storage characteristics of different tablespace locations.

The structure uses PostgreSQL's varlena format, making it a variable-length structure that can be efficiently stored and retrieved. The cost parameters influence the query planner's decisions about execution strategies, while the concurrency parameters control parallel I/O operations for both regular queries and maintenance operations.

These options can be set using the `CREATE TABLESPACE` statement with the `WITH` clause or modified using `ALTER TABLESPACE SET`. The values override the corresponding global configuration parameters for objects stored in the specific tablespace.

## Parameters / Member Variables
- `vl_len_`: Standard varlena header containing the total length of the structure; should not be manipulated directly by user code
- `random_page_cost`: Cost estimate for random page access, influencing the planner's preference for index scans vs. sequential scans
- `seq_page_cost`: Cost estimate for sequential page access, affecting decisions about table scan strategies  
- `effective_io_concurrency`: Number of concurrent disk I/O operations that PostgreSQL expects the storage subsystem to handle efficiently for regular queries
- `maintenance_io_concurrency`: Number of concurrent disk I/O operations expected during maintenance operations like VACUUM, CREATE INDEX, and ALTER TABLE

## Dependencies
- Functions called/Symbols referenced:
  - (None directly referenced from the structure definition)

- Called from (representative examples):
  - tablespace_reloptions (src/backend/access/common/reloptions.c:2098-2106)
  - get_tablespace (src/backend/utils/cache/spccache.c:111)

## Notes and Other Information
- This structure is stored as part of the tablespace's metadata in the PostgreSQL system catalogs
- The varlena format allows for future extensibility by adding new options without breaking compatibility
- Default values for these parameters come from the corresponding global GUC (Grand Unified Configuration) parameters
- Setting appropriate values based on storage characteristics (SSD vs. HDD, local vs. networked storage) can significantly impact query performance
- The structure is cached by the tablespace cache system for efficient access during query planning and execution