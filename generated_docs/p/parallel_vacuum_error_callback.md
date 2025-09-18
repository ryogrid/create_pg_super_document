# parallel_vacuum_error_callback

## Location
[src/backend/commands/vacuumparallel.c:1105-1128](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/vacuumparallel.c#L1105-L1128)

## Overview
Error context callback function that provides meaningful error messages during parallel index vacuum operations by identifying the specific index and relation being processed when an error occurs.

## Definition


## Detailed Description
This function is registered as an error context callback to enhance error reporting during parallel vacuum operations. When an error occurs during index processing, this callback provides contextual information about which specific index and relation were being processed, making debugging and error diagnosis much easier.

The function examines the current parallel vacuum status and generates appropriate error context messages:

**For Bulk Delete Operations** ():
- Reports "while vacuuming index [index_name] of relation [namespace.relation_name]"

**For Cleanup Operations** ():
- Reports "while cleaning up index [index_name] of relation [namespace.relation_name]"

**For Other States**:
- Returns without adding context (initial or completed states don't need error context)

The error messages are designed to match those used in the sequential vacuum error context () to maintain consistency in error reporting across both parallel and non-parallel vacuum operations.

## Parameters / Member Variables
- : Void pointer to ParallelVacuumState structure containing current vacuum context information

## Dependencies
- Functions called/Symbols referenced:
  - errcontext
  - PARALLEL_INDVAC_STATUS_NEED_BULKDELETE
  - PARALLEL_INDVAC_STATUS_NEED_CLEANUP
  - PARALLEL_INDVAC_STATUS_INITIAL
  - PARALLEL_INDVAC_STATUS_COMPLETED
- Called from (representative examples):
  - [parallel_vacuum_main](parallel_vacuum_main.md) (registered as error callback)

## Notes and Other Information
- This is a static function used internally within the parallel vacuum implementation
- The function is registered with the error context stack in 
- Error messages intentionally match those in  for consistency
- Only provides context for active vacuum operations (bulk delete and cleanup phases)
- The ParallelVacuumState structure is updated with current index name and status before operations begin
- Essential for debugging parallel vacuum issues by pinpointing exactly which index was being processed when an error occurred