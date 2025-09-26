# ReindexParams

## Location
[src/include/catalog/index.h:33-38](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/catalog/index.h#L33-L38)

## Overview
A structure that encapsulates configuration parameters for REINDEX operations, including option flags and tablespace specifications.

## Definition
typedef struct ReindexParams
{
    bits32      options;        /* bitmask of REINDEXOPT_* */
    Oid         tablespaceOid;  /* New tablespace to move indexes to.
                                 * InvalidOid to do nothing. */
} ReindexParams;

## Detailed Description
ReindexParams is a parameter structure used throughout PostgreSQL's reindexing subsystem to pass configuration options and settings for REINDEX operations. It provides a consistent interface for specifying how indexes should be rebuilt, including behavioral options and target tablespace settings.

The structure centralizes reindex configuration, allowing various reindex functions to receive consistent parameters and enabling extensibility for future reindexing options.

## Parameters / Member Variables
- `options`: A bitmask containing REINDEXOPT_* flags that control reindex behavior:
  - REINDEXOPT_CONCURRENTLY: Perform reindex concurrently without blocking reads/writes
  - REINDEXOPT_MISSING_OK: Don't error if the index to reindex doesn't exist
  - REINDEXOPT_REPORT_PROGRESS: Enable progress reporting during reindex
  - REINDEXOPT_VERBOSE: Enable verbose output during reindex operations
- `tablespaceOid`: OID of the target tablespace to move indexes to during reindex, or InvalidOid to keep indexes in their current tablespace

## Dependencies
- Functions called/Symbols referenced:
  - bits32
- Called from (representative examples):
  - reindex_index
  - reindex_relation
  - finish_heap_swap
  - ExecReindex
  - ReindexIndex
  - ReindexTable
  - ReindexMultipleTables
  - ReindexPartitions
  - ReindexRelationConcurrently

## Notes and Other Information
- This structure is widely used across the reindexing subsystem, appearing in functions for reindexing individual indexes, tables, and multiple objects
- The options bitmask allows combining multiple REINDEXOPT_* flags using bitwise OR operations
- When tablespaceOid is InvalidOid, indexes remain in their current tablespace during reindexing
- The structure provides a clean API for passing reindex parameters between different layers of the indexing system
- Used in both user-initiated REINDEX commands and internal reindexing operations like during table clustering