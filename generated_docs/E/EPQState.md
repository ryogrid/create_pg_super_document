# EPQState

## Location
[src/include/nodes/execnodes.h:1252-1313](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/execnodes.h#L1252-L1313)

## Overview
EPQState manages the execution state for EvalPlanQual (EPQ) operations, which recheck candidate tuples during concurrent modifications to ensure transaction isolation and consistency.

## Definition

```c
typedef struct EPQState
{
	/* These are initialized by EvalPlanQualInit() and do not change later: */
	EState	   *parentestate;	/* main query's EState */
	int			epqParam;		/* ID of Param to force scan node re-eval */
	List	   *resultRelations;	/* integer list of RT indexes, or NIL */

	/*
	 * relsubs_slot[scanrelid - 1] holds the EPQ test tuple to be returned by
	 * the scan node for the scanrelid'th RT index, in place of performing an
	 * actual table scan.  Callers should use EvalPlanQualSlot() to fetch
	 * these slots.
	 */
	List	   *tuple_table;	/* tuple table for relsubs_slot */
	TupleTableSlot **relsubs_slot;

	/*
	 * Initialized by EvalPlanQualInit(), may be changed later with
	 * EvalPlanQualSetPlan():
	 */

	Plan	   *plan;			/* plan tree to be executed */
	List	   *arowMarks;		/* ExecAuxRowMarks (non-locking only) */


	/*
	 * The original output tuple to be rechecked.  Set by
	 * EvalPlanQualSetSlot(), before EvalPlanQualNext() or EvalPlanQual() may
	 * be called.
	 */
	TupleTableSlot *origslot;


	/* Initialized or reset by EvalPlanQualBegin(): */

	EState	   *recheckestate;	/* EState for EPQ execution, see above */

	/*
	 * Rowmarks that can be fetched on-demand using
	 * EvalPlanQualFetchRowMark(), indexed by scanrelid - 1. Only non-locking
	 * rowmarks.
	 */
	ExecAuxRowMark **relsubs_rowmark;

	/*
	 * relsubs_done[scanrelid - 1] is true if there is no EPQ tuple for this
	 * target relation or it has already been fetched in the current scan of
	 * this target relation within the current EvalPlanQual test.
	 */
	bool	   *relsubs_done;

	/*
	 * relsubs_blocked[scanrelid - 1] is true if there is no EPQ tuple for
	 * this target relation during the current EvalPlanQual test.  We keep
	 * these flags set for all relids listed in resultRelations, but
	 * transiently clear the one for the relation whose tuple is actually
	 * passed to EvalPlanQual().
	 */
	bool	   *relsubs_blocked;

	PlanState  *recheckplanstate;	/* EPQ specific exec nodes, for ->plan */
} EPQState;
```
## Detailed Description
EPQState implements EvalPlanQual (EPQ) rechecking, a critical mechanism in PostgreSQL's Multi-Version Concurrency Control (MVCC) system. When a transaction attempts to update or delete a tuple that has been concurrently modified by another transaction, EPQ creates a separate execution environment to recheck the candidate tuple against the original query conditions. This ensures that the query's WHERE clause and join conditions are still satisfied after concurrent modifications, maintaining transaction isolation without unnecessary blocking.

## Parameters / Member Variables
- : Pointer to the main query's execution state, providing shared resources like range tables
- : Parameter ID used to force re-evaluation of scan nodes during EPQ execution
- : List of range table indexes for relations that may need EPQ rechecking
- : Tuple table structure managing the relsubs_slot array
- : Array of slots containing EPQ test tuples, indexed by scanrelid - 1
- : Plan tree that needs to be rechecked during EPQ execution
- : List of ExecAuxRowMarks for non-locking row marking operations
- : Original output tuple being rechecked, set before EPQ evaluation begins
- : Separate EState for EPQ execution, sharing resources with parentestate
- : Array of row marks that can be fetched on-demand, indexed by scanrelid - 1
- : Array of flags indicating whether EPQ tuple has been fetched for each relation
- : Array of flags indicating relations with no EPQ tuple during current test
- : Execution state tree for the plan being rechecked, separate from main query

## Dependencies
- Functions called/Symbols referenced:
  - [EState](EState.md)
  - [List](../L/List.md)
  - TupleTableSlot
  - [Plan](../P/Plan.md)
  - [ExecAuxRowMark](ExecAuxRowMark.md)
  - [PlanState](../P/PlanState.md)
- Called from (representative examples):
  - [EvalPlanQual](EvalPlanQual.md)
  - [EvalPlanQualInit](EvalPlanQualInit.md)
  - [EvalPlanQualBegin](EvalPlanQualBegin.md)
  - [EvalPlanQualNext](EvalPlanQualNext.md)
  - [ExecMergeMatched](ExecMergeMatched.md)
  - [GetTupleForTrigger](../G/GetTupleForTrigger.md)

## Notes and Other Information
EPQState is fundamental to PostgreSQL's optimistic concurrency control, allowing transactions to proceed without locking while maintaining consistency through rechecking. The separate execution environment (recheckestate) enables EPQ to run modified plans that use substitute tuples instead of scanning base tables. The relsubs_slot mechanism allows callers to provide specific tuples for rechecking, while the rowmark system handles tuple identification. EPQ is essential for operations like UPDATE, DELETE, MERGE, and trigger execution where concurrent modifications must be handled gracefully. The blocked/done flag arrays optimize EPQ execution by tracking which relations have viable tuples for the current recheck operation.