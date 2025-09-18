# MergeJoinState

## Location
[src/include/nodes/execnodes.h:2136-2156](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/execnodes.h#L2136-L2156)

## Overview
MergeJoinState represents the execution state for merge join operations, maintaining all state information needed to perform sorted merge joins between two pre-sorted input relations.

## Definition


## Detailed Description
MergeJoinState extends JoinState to provide comprehensive state management for merge join execution. Merge joins are efficient for joining two relations that are already sorted on the join keys, using a parallel scan algorithm similar to merging sorted arrays. The state maintains join clauses for multi-column joins, tracks the current position in the merge algorithm state machine, manages tuple slots for current and marked positions, and handles optimization flags for performance improvements.

## Parameters / Member Variables
- : Base JoinState structure containing common join execution state
- : Number of join clauses (for multi-column joins)
- : Array of MergeJoinClause structures, one for each join condition
- : Current state of the merge join state machine (tracks algorithm progress)
- : Optimization flag - true if Mark and Restore operations can be skipped
- : True to issue extra Mark operations on inner scan for optimization
- : True if there is a constant-false join qualification (optimization)
- : True if unjoined outer tuples should be emitted (for left/full outer joins)
- : True if unjoined inner tuples should be emitted (for right/full outer joins)
- : True if a join match has been found for the current outer tuple
- : True if a join match has been found for the current inner tuple
- : Tuple slot for the current outer relation tuple
- : Tuple slot for the current inner relation tuple
- : Tuple slot for the marked position (used for backtracking)
- : Pre-prepared null tuple for right outer joins
- : Pre-prepared null tuple for left outer joins
- : Expression context for computing outer tuple join values
- : Expression context for computing inner tuple join values

## Dependencies
- Functions called/Symbols referenced:
  - [JoinState](../J/JoinState.md) (inherited base structure)
  - [MergeJoinClause](MergeJoinClause.md) (for join clause specifications)
  - TupleTableSlot (for tuple storage)
  - ExprContext (for expression evaluation contexts)
- Called from (representative examples):
  - ExecMergeJoin (main execution function)
  - ExecInitMergeJoin (initialization function)
  - ExecEndMergeJoin (cleanup function)
  - ExecReScanMergeJoin (rescan function)
  - [MJCompare](MJCompare.md) (comparison function)
  - [MJFillOuter](MJFillOuter.md)/MJFillInner (outer join handling)

## Notes and Other Information
- Merge joins require both input relations to be sorted on the join keys, making them efficient for large sorted datasets
- The state machine approach handles the complex logic of advancing through both relations while handling duplicates and outer join semantics
- Mark and Restore functionality allows the algorithm to backtrack when multiple matching tuples exist
- The algorithm is particularly efficient when join selectivity is high and both relations are already sorted
- Performance optimizations include skipping mark/restore when possible and detecting constant-false conditions early
- The structure is defined in src/include/nodes/execnodes.h at lines 2136-2156