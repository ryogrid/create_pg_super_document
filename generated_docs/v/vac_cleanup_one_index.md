# vac_cleanup_one_index

## Location
src/backend/commands/vacuum.c: 2558 - 2583

## Overview
Performs post-vacuum cleanup operations on a single index relation, handling the finalization phase of index vacuum processing and reporting statistics about the cleanup results.

## Definition


## Detailed Description
The `vac_cleanup_one_index` function serves as a wrapper around `index_vacuum_cleanup` to perform post-deletion cleanup operations on an index after bulk deletion has occurred. This function is responsible for finalizing the vacuum process on a single index and providing detailed reporting about the cleanup results.

The function delegates the actual cleanup work to `index_vacuum_cleanup`, then generates a comprehensive status report about the index's state after cleanup. The reporting includes information about remaining row versions, deleted pages, and reusable space, which is valuable for monitoring vacuum effectiveness and index health.

This function is typically called during the cleanup phase of vacuum operations, whether in sequential or parallel vacuum processing contexts.

## Parameters / Member Variables
- `ivinfo`: IndexVacuumInfo structure containing vacuum context information for the index, including the index relation, message level for reporting, and other vacuum-specific parameters
- `istat`: IndexBulkDeleteResult structure containing statistics from previous bulk deletion operations, which may be NULL if no prior bulk delete occurred

## Dependencies
- Functions called/Symbols referenced:
  - [index_vacuum_cleanup](../i/index_vacuum_cleanup.md): Core function that performs the actual index cleanup operations
  - [IndexVacuumInfo](../I/IndexVacuumInfo.md): Structure type containing vacuum context information
  - [IndexBulkDeleteResult](../I/IndexBulkDeleteResult.md): Structure type containing bulk deletion statistics
  - `ereport`: PostgreSQL error/message reporting function
  - `RelationGetRelationName`: Utility function to get relation name for reporting

- Called from (representative examples):
  - [lazy_cleanup_one_index](../l/lazy_cleanup_one_index.md): Called during heap vacuum lazy cleanup phase
  - [parallel_vacuum_process_one_index](../p/parallel_vacuum_process_one_index.md): Called during parallel vacuum processing

## Notes and Other Information
- The function always returns the `IndexBulkDeleteResult` structure, which may be the same as the input or a new/modified structure from `index_vacuum_cleanup`
- Detailed statistics reporting only occurs when `istat` is non-NULL, providing transparency into vacuum effectiveness
- The reporting includes both summary statistics (total row versions, pages) and detailed breakdown (removed tuples, deleted pages, reusable pages)
- This function is part of PostgreSQL's vacuum subsystem and plays a crucial role in maintaining index health and reclaiming space
- The message level for reporting is controlled by the `ivinfo->message_level` setting, allowing for configurable verbosity