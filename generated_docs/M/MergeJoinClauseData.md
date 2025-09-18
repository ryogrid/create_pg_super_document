# MergeJoinClauseData

## Location
src/backend/executor/nodeMergejoin.c: 120 - 140

## Overview
MergeJoinClauseData is a runtime data structure that encapsulates all the information needed to execute a single merge join clause, including expression states, cached values, and comparison functions.

## Definition


## Detailed Description
MergeJoinClauseData represents the runtime state for a single merge join clause during execution. Each instance corresponds to one equality condition in the merge join's join quals. The structure is designed to optimize performance by:

1. **Pre-compiled expressions**: The lexpr and rexpr fields contain pre-compiled expression states that can be efficiently evaluated during join execution.

2. **Cached evaluation results**: The ldatum/rdatum and lisnull/risnull fields cache the most recently computed values from the expressions, avoiding redundant evaluations when the same tuple is compared multiple times.

3. **Optimized comparison**: The SortSupportData provides fast comparison functions that are set up once during initialization and used repeatedly during join processing.

The structure is created and initialized by MJExamineQuals() during merge join node initialization, with one MergeJoinClauseData instance created for each mergejoinable condition provided by the planner.

## Parameters / Member Variables
- : Pre-compiled expression state for evaluating the left-hand (outer relation) side of the join condition
- : Pre-compiled expression state for evaluating the right-hand (inner relation) side of the join condition  
- : Cached result value from the most recent evaluation of lexpr
- : Cached result value from the most recent evaluation of rexpr
- : NULL indicator flag for ldatum
- : NULL indicator flag for rdatum
- : Sort support data containing comparison functions, collation info, sort direction, and null handling rules for efficiently comparing ldatum and rdatum

## Dependencies
- Functions called/Symbols referenced:
  - SortSupportData
  - ExprState
  - Datum
- Called from (representative examples):
  - [MJExamineQuals](MJExamineQuals.md) (creates and initializes arrays of MergeJoinClauseData)
  - [MergeJoinClause](MergeJoinClause.md) (typedef pointer to this structure)

## Notes and Other Information
- This structure is private to nodeMergejoin.c and is not exposed in header files
- The structure is designed for high-performance tuple-by-tuple comparison during merge join execution
- The SortSupportData member provides access to optimized comparison routines that can handle different data types, collations, and sort orders
- Memory for arrays of these structures is allocated in the query's per-tuple memory context during node initialization
- The cached datum values are updated as the merge join algorithm advances through the sorted input streams