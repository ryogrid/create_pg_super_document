# index_vacuum_cleanup

## Location
[src/backend/access/index/indexam.c:769-787](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/index/indexam.c#L769-L787)

## Overview
Performs post-deletion cleanup operations on an index after bulk deletion, finalizing the vacuum process and updating index statistics.

## Definition
```c
IndexBulkDeleteResult *index_vacuum_cleanup(IndexVacuumInfo *info,
                                            IndexBulkDeleteResult *istat)
```

## Detailed Description
The `index_vacuum_cleanup` function is the final phase of the index vacuum process, responsible for performing cleanup operations after bulk deletions have been completed. This function is typically called after `index_bulk_delete` to finalize the vacuum operation on an index.

The cleanup phase allows index access methods to perform necessary maintenance tasks such as:
- Reclaiming freed space and consolidating index pages
- Updating index statistics and metadata
- Finalizing any deferred operations from the bulk delete phase
- Preparing the index for optimal future operations

Like other index access method functions, it delegates the actual work to the access method's specific `amvacuumcleanup` procedure, allowing different index types to implement cleanup strategies tailored to their internal structure and optimization needs.

## Parameters / Member Variables
- `info`: IndexVacuumInfo structure containing index vacuum context, including the index relation and vacuum parameters
- `istat`: IndexBulkDeleteResult structure containing accumulated statistics from previous bulk delete operations (can be NULL if no bulk delete was performed)

## Dependencies
- Functions called/Symbols referenced:
  - RELATION_CHECKS (macro for relation validation)
  - CHECK_REL_PROCEDURE (macro to verify amvacuumcleanup procedure exists)
  - amvacuumcleanup (access method specific vacuum cleanup procedure)
- Called from (representative examples):
  - [do_analyze_rel](../d/do_analyze_rel.md) (table analysis operations)
  - [vac_cleanup_one_index](../v/vac_cleanup_one_index.md) (vacuum cleanup for single index)
  - IndexScanIsValid (index scan validation)

## Notes and Other Information
- Returns an optional palloc'd IndexBulkDeleteResult structure with final statistics
- Called even when no bulk deletion occurred, allowing indexes to perform routine maintenance
- Essential for maintaining optimal index performance by allowing access methods to reorganize and optimize their structure
- The cleanup phase may be more expensive than bulk deletion for some index types, as it performs structural optimizations
- Different index types use this opportunity for various maintenance: B-trees may consolidate pages, GIN may merge posting lists, etc.
- Part of PostgreSQL's two-phase vacuum approach: bulk delete followed by cleanup for maximum efficiency