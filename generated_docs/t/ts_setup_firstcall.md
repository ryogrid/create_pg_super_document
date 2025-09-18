# ts_setup_firstcall

## Location
src/backend/utils/adt/tsvector_op.c: 2448 - 2488

## Overview
Sets up the function call context for the first call of a set-returning function that iterates over TSVectorStat entries using an in-order traversal of the statistics tree.

## Definition


## Detailed Description
This function initializes the function call context for set-returning functions that traverse TSVectorStat trees. It sets up a stack-based traversal mechanism to perform an in-order walk of the binary tree structure containing text search statistics. The function allocates memory for the traversal stack, finds the leftmost node in the tree as the starting point, and configures the tuple descriptor for returning composite row types.

The function operates within PostgreSQL's set-returning function (SRF) framework, preparing the necessary data structures for iterative calls that will return one row at a time from the statistics tree.

## Parameters / Member Variables
- `fcinfo`: Function call information structure containing call context and parameters
- `funcctx`: Function call context for managing state across multiple calls
- `stat`: TSVectorStat structure containing the root of the statistics tree and metadata

## Dependencies
- Functions called/Symbols referenced:
  - MemoryContextSwitchTo
  - palloc0
  - get_call_result_type
  - TupleDescGetAttInMetadata
  - elog
- Called from (representative examples):
  - ts_stat1
  - ts_stat2

## Notes and Other Information
- Uses a stack-based approach to maintain traversal state between function calls
- Implements in-order traversal by initially finding the leftmost node in the tree
- Memory allocation occurs in the multi-call memory context to persist across calls
- Validates that the return type is composite (row type) as required for SRFs
- The stack size is allocated based on the maximum depth of the tree to prevent overflow
- Part of PostgreSQL's text search functionality for analyzing TSVector statistics