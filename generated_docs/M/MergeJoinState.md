# MergeJoinState

## Location
[src/include/nodes/execnodes.h:2136-2156](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/execnodes.h#L2136-L2156)

## Overview
MergeJoinState represents the execution state for merge join operations, maintaining all state information needed to perform sorted merge joins between two pre-sorted input relations.

## Definition

```c
typedef struct MergeJoinState
{
	JoinState	js;				/* its first field is NodeTag */
	int			mj_NumClauses;
	MergeJoinClause mj_Clauses; /* array of length mj_NumClauses */
	int			mj_JoinState;
	bool		mj_SkipMarkRestore;
	bool		mj_ExtraMarks;
	bool		mj_ConstFalseJoin;
	bool		mj_FillOuter;
	bool		mj_FillInner;
	bool		mj_MatchedOuter;
	bool		mj_MatchedInner;
	TupleTableSlot *mj_OuterTupleSlot;
	TupleTableSlot *mj_InnerTupleSlot;
	TupleTableSlot *mj_MarkedTupleSlot;
	TupleTableSlot *mj_NullOuterTupleSlot;
	TupleTableSlot *mj_NullInnerTupleSlot;
	ExprContext *mj_OuterEContext;
	ExprContext *mj_InnerEContext;
} MergeJoinState;
```
## Detailed Description
MergeJoinState extends JoinState to provide comprehensive state management for merge join execution. Merge joins are efficient for joining two relations that are already sorted on the join keys, using a parallel scan algorithm similar to merging sorted arrays. The state maintains join clauses for multi-column joins, tracks the current position in the merge algorithm state machine, manages tuple slots for current and marked positions, and handles optimization flags for performance improvements.

## Parameters / Member Variables
- `js`: Base JoinState structure containing common join execution state
- `mj_NumClauses`: Number of join clauses (for multi-column joins)
- `mj_Clauses`: Array of MergeJoinClause structures, one for each join condition
- `mj_JoinState`: Current state of the merge join state machine (tracks algorithm progress)
- `mj_SkipMarkRestore`: Optimization flag - true if Mark and Restore operations can be skipped
- `mj_ExtraMarks`: True to issue extra Mark operations on inner scan for optimization
- `mj_ConstFalseJoin`: True if there is a constant-false join qualification (optimization)
- `mj_FillOuter`: True if unjoined outer tuples should be emitted (for left/full outer joins)
- `mj_FillInner`: True if unjoined inner tuples should be emitted (for right/full outer joins)
- `mj_MatchedOuter`: True if a join match has been found for the current outer tuple
- `mj_MatchedInner`: True if a join match has been found for the current inner tuple
- `*mj_OuterTupleSlot`: Tuple slot for the current outer relation tuple
- `*mj_InnerTupleSlot`: Tuple slot for the current inner relation tuple
- `*mj_MarkedTupleSlot`: Tuple slot for the marked position (used for backtracking)
- `*mj_NullOuterTupleSlot`: Pre-prepared null tuple for right outer joins
- `*mj_NullInnerTupleSlot`: Pre-prepared null tuple for left outer joins
- `*mj_OuterEContext`: Expression context for computing outer tuple join values
- `*mj_InnerEContext`: Expression context for computing inner tuple join values
## Dependencies
- Functions called/Symbols referenced:
  - [JoinState](../J/JoinState.md) (inherited base structure)
  - [MergeJoinClause](MergeJoinClause.md) (for join clause specifications)
  - [TupleTableSlot](../T/TupleTableSlot.md) (for tuple storage)
  - [ExprContext](../E/ExprContext.md) (for expression evaluation contexts)
- Called from (representative examples):
  - [ExecMergeJoin](../E/ExecMergeJoin.md) (main execution function)
  - [ExecInitMergeJoin](../E/ExecInitMergeJoin.md) (initialization function)
  - [ExecEndMergeJoin](../E/ExecEndMergeJoin.md) (cleanup function)
  - [ExecReScanMergeJoin](../E/ExecReScanMergeJoin.md) (rescan function)
  - [MJCompare](MJCompare.md) (comparison function)
  - [MJFillOuter](MJFillOuter.md)/MJFillInner (outer join handling)

## Notes and Other Information
- Merge joins require both input relations to be sorted on the join keys, making them efficient for large sorted datasets
- The state machine approach handles the complex logic of advancing through both relations while handling duplicates and outer join semantics
- Mark and Restore functionality allows the algorithm to backtrack when multiple matching tuples exist
- The algorithm is particularly efficient when join selectivity is high and both relations are already sorted
- Performance optimizations include skipping mark/restore when possible and detecting constant-false conditions early
- The structure is defined in src/include/nodes/execnodes.h at lines 2136-2156