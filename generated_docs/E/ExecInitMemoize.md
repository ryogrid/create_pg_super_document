# ExecInitMemoize

## Location
src/backend/executor/nodeMemoize.c: 952 - 1079

## Overview
ExecInitMemoize initializes a MemoizeState node for the Memoize executor, setting up hash functions, expression contexts, memory management, and all necessary data structures for parameter-based result caching.

## Definition


## Detailed Description
ExecInitMemoize performs comprehensive initialization of a Memoize node's runtime state. It sets up the hash table infrastructure for caching, initializes expression evaluation for cache key parameters, configures memory management with appropriate limits, and establishes the LRU (Least Recently Used) eviction mechanism. The function creates specialized tuple slots for both hash key storage and result caching, builds hash and equality functions for parameter comparison, and initializes all statistics counters. It defers the actual hash table allocation until execution time to avoid unnecessary memory consumption during planning.

## Parameters / Member Variables
- `node`: The Memoize plan node containing configuration like parameter expressions, hash operators, collations, and optimization flags
- `estate`: The execution state containing the query's execution context, memory contexts, and global execution information  
- `eflags`: Execution flags that control node behavior (EXEC_FLAG_BACKWARD and EXEC_FLAG_MARK are not supported)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode
  - ExecAssignExprContext
  - outerPlan
  - [ExecInitNode](ExecInitNode.md)
  - [ExecInitResultTupleSlotTL](ExecInitResultTupleSlotTL.md)
  - ExecCreateScanSlotFromOuterPlan
  - [ExecTypeFromExprList](ExecTypeFromExprList.md)
  - [MakeSingleTupleTableSlot](../M/MakeSingleTupleTableSlot.md)
  - [get_op_hash_functions](../g/get_op_hash_functions.md)
  - [fmgr_info](../f/fmgr_info.md)
  - [ExecInitExpr](ExecInitExpr.md)
  - [get_opcode](../g/get_opcode.md)
  - [ExecBuildParamSetEqual](ExecBuildParamSetEqual.md)
  - [get_hash_memory_limit](../g/get_hash_memory_limit.md)
  - AllocSetContextCreate
  - [dlist_init](../d/dlist_init.md)
- Called from (representative examples):
  - [ExecInitNode](ExecInitNode.md) (main node initialization dispatcher)

## Notes and Other Information
- Validates that unsupported execution flags (EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK) are not set since Memoize doesn't support backward scanning or mark/restore
- Creates separate memory context "MemoizeHashTable" for cache data to enable easy cleanup and memory tracking
- Supports both binary mode (bit-by-bit key comparison) and logical mode (using type-specific equality operators) for parameter matching
- Optimizes for single-row scenarios where cache entries can be marked complete after the first tuple
- Uses TTSOpsMinimalTuple for efficient storage and TTSOpsVirtual for temporary key operations
- Initializes comprehensive instrumentation counters for monitoring cache performance