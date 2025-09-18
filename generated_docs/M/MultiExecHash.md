# MultiExecHash

## Location
src/backend/executor/nodeHash.c: 105 - 137

## Overview
MultiExecHash is the main execution function for Hash nodes that builds hash tables for hash joins, supporting both single-backend and parallel execution modes with optional partitioning for multiple batches.

## Definition


## Detailed Description
MultiExecHash serves as the primary execution entry point for Hash nodes in PostgreSQL's executor. Unlike typical executor nodes that return tuples one-by-one, this function consumes all input tuples from its child node to build a complete hash table that will be used by the parent HashJoin node.

The function intelligently delegates to either MultiExecPrivateHash (for single-backend execution) or MultiExecParallelHash (for parallel execution) based on whether parallel_state is configured. It provides its own instrumentation support for performance monitoring, tracking the number of tuples processed.

The function intentionally returns NULL rather than the hash table directly, as hash tables are not Node subtypes and would violate the MultiExecProcNode API. The parent HashJoin node is expected to access the hash table directly from the HashState structure.

## Parameters / Member Variables
- : HashState pointer containing execution state and configuration for the Hash node

## Dependencies
- Functions called/Symbols referenced:
  - HashState (parameter type)
  - InstrStartNode (instrumentation start)
  - MultiExecParallelHash (parallel execution path)
  - MultiExecPrivateHash (single-backend execution path)  
  - InstrStopNode (instrumentation end)
- Called from (representative examples):
  - MultiExecProcNode (main executor dispatch)
  - NODEHASH_H (header declaration)

## Notes and Other Information
- Returns NULL to comply with MultiExecProcNode API while the actual hash table is stored in node->hashtable
- Handles both parallel and non-parallel execution transparently
- Provides manual instrumentation since Hash nodes don't follow standard execution patterns
- The hash table building process may involve partitioning data into multiple batches if memory constraints require it
- Located in src/backend/executor/nodeHash.c:105-137