# pg_get_backend_memory_contexts

## Location
src/backend/utils/adt/mcxtfuncs.c: 119 - 143

## Overview
SQL-callable function that returns memory context information for the current backend process as a set-returning function (SRF).

## Definition


## Detailed Description
This function implements the backend logic for the  system view. It traverses the memory context hierarchy starting from  and collects statistics about memory usage, including total bytes, free bytes, number of blocks, and hierarchical relationships between contexts. The function uses PostgreSQL's SRF (Set Returning Function) mechanism to return multiple rows of memory context data.

The function initializes a materialized SRF and delegates the actual memory context traversal to , which recursively walks through the memory context tree and populates the result set with context statistics.

## Parameters / Member Variables
- This function takes standard PostgreSQL function arguments via  macro
- Returns memory context data with columns: name, ident, parent, level, total_bytes, total_nblocks, free_bytes, free_chunks, used_bytes

## Dependencies
- Functions called/Symbols referenced:
  - [InitMaterializedSRF](../I/InitMaterializedSRF.md)
  - PutMemoryContextsStatsTupleStore
  - ReturnSetInfo (struct type)
- Called from (representative examples):
  - pg_backend_memory_contexts system view (via SQL queries)

## Notes and Other Information
- This function is only accessible to users with  role by default
- The function provides a snapshot of memory contexts at the time of execution
- Memory context hierarchy is traversed starting from TopMemoryContext
- Used primarily for debugging memory usage and detecting memory leaks in PostgreSQL backends
- The output is used by the  system view defined in system_views.sql