# ResultRelInfo

## Location
[src/include/nodes/execnodes.h:450-596](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/execnodes.h#L450-L596)

## Overview
ResultRelInfo holds comprehensive information about a result relation, including indexes, triggers, and state needed for INSERT, UPDATE, DELETE, and MERGE operations.

## Definition

```c
typedef struct ResultRelInfo
{
	NodeTag		type;

	/* result relation's range table index, or 0 if not in range table */
	Index		ri_RangeTableIndex;

	/* relation descriptor for result relation */
	Relation	ri_RelationDesc;

	/* # of indices existing on result relation */
	int			ri_NumIndices;

	/* array of relation descriptors for indices */
	RelationPtr ri_IndexRelationDescs;

	/* array of key/attr info for indices */
	IndexInfo **ri_IndexRelationInfo;

	/*
	 * For UPDATE/DELETE result relations, the attribute number of the row
	 * identity junk attribute in the source plan's output tuples
	 */
	AttrNumber	ri_RowIdAttNo;

	/* For UPDATE, attnums of generated columns to be computed */
	Bitmapset  *ri_extraUpdatedCols;

	/* Projection to generate new tuple in an INSERT/UPDATE */
	ProjectionInfo *ri_projectNew;
	/* Slot to hold that tuple */
	TupleTableSlot *ri_newTupleSlot;
	/* Slot to hold the old tuple being updated */
	TupleTableSlot *ri_oldTupleSlot;
	/* Have the projection and the slots above been initialized? */
	bool		ri_projectNewInfoValid;

	/* updates do LockTuple() before oldtup read; see README.tuplock */
	bool		ri_needLockTagTuple;

	/* triggers to be fired, if any */
	TriggerDesc *ri_TrigDesc;

	/* cached lookup info for trigger functions */
	FmgrInfo   *ri_TrigFunctions;

	/* array of trigger WHEN expr states */
	ExprState **ri_TrigWhenExprs;

	/* optional runtime measurements for triggers */
	Instrumentation *ri_TrigInstrument;

	/* On-demand created slots for triggers / returning processing */
	TupleTableSlot *ri_ReturningSlot;	/* for trigger output tuples */
	TupleTableSlot *ri_TrigOldSlot; /* for a trigger's old tuple */
	TupleTableSlot *ri_TrigNewSlot; /* for a trigger's new tuple */

	/* FDW callback functions, if foreign table */
	struct FdwRoutine *ri_FdwRoutine;

	/* available to save private state of FDW */
	void	   *ri_FdwState;

	/* true when modifying foreign table directly */
	bool		ri_usesFdwDirectModify;

	/* batch insert stuff */
	int			ri_NumSlots;	/* number of slots in the array */
	int			ri_NumSlotsInitialized; /* number of initialized slots */
	int			ri_BatchSize;	/* max slots inserted in a single batch */
	TupleTableSlot **ri_Slots;	/* input tuples for batch insert */
	TupleTableSlot **ri_PlanSlots;

	/* list of WithCheckOption's to be checked */
	List	   *ri_WithCheckOptions;

	/* list of WithCheckOption expr states */
	List	   *ri_WithCheckOptionExprs;

	/* array of constraint-checking expr states */
	ExprState **ri_ConstraintExprs;

	/* arrays of stored generated columns expr states, for INSERT and UPDATE */
	ExprState **ri_GeneratedExprsI;
	ExprState **ri_GeneratedExprsU;

	/* number of stored generated columns we need to compute */
	int			ri_NumGeneratedNeededI;
	int			ri_NumGeneratedNeededU;

	/* list of RETURNING expressions */
	List	   *ri_returningList;

	/* for computing a RETURNING list */
	ProjectionInfo *ri_projectReturning;

	/* list of arbiter indexes to use to check conflicts */
	List	   *ri_onConflictArbiterIndexes;

	/* ON CONFLICT evaluation state */
	OnConflictSetState *ri_onConflict;

	/* for MERGE, lists of MergeActionState (one per MergeMatchKind) */
	List	   *ri_MergeActions[NUM_MERGE_MATCH_KINDS];

	/* for MERGE, expr state for checking the join condition */
	ExprState  *ri_MergeJoinCondition;

	/* partition check expression state (NULL if not set up yet) */
	ExprState  *ri_PartitionCheckExpr;

	/*
	 * Map to convert child result relation tuples to the format of the table
	 * actually mentioned in the query (called "root").  Computed only if
	 * needed.  A NULL map value indicates that no conversion is needed, so we
	 * must have a separate flag to show if the map has been computed.
	 */
	TupleConversionMap *ri_ChildToRootMap;
	bool		ri_ChildToRootMapValid;

	/*
	 * As above, but in the other direction.
	 */
	TupleConversionMap *ri_RootToChildMap;
	bool		ri_RootToChildMapValid;

	/*
	 * Other information needed by child result relations
	 *
	 * ri_RootResultRelInfo gives the target relation mentioned in the query.
	 * Used as the root for tuple routing and/or transition capture.
	 *
	 * ri_PartitionTupleSlot is non-NULL if the relation is a partition to
	 * route tuples into and ri_RootToChildMap conversion is needed.
	 */
	struct ResultRelInfo *ri_RootResultRelInfo;
	TupleTableSlot *ri_PartitionTupleSlot;

	/* for use by copyfrom.c when performing multi-inserts */
	struct CopyMultiInsertBuffer *ri_CopyMultiInsertBuffer;

	/*
	 * Used when a leaf partition is involved in a cross-partition update of
	 * one of its ancestors; see ExecCrossPartitionUpdateForeignKey().
	 */
	List	   *ri_ancestorResultRels;
} ResultRelInfo;
```
## Detailed Description
ResultRelInfo is a comprehensive structure that holds all information needed about a result relation for data modification operations. When updating an existing relation, PostgreSQL must also update indexes and potentially fire triggers. This structure centralizes all the necessary state.

ResultRelInfo can refer to tables in the query's range table (with ri_RangeTableIndex set) or to relations not in the range table, such as partition targets for tuple routing or trigger target tables. The structure supports complex operations including batch inserts, MERGE statements, ON CONFLICT handling, partitioning, and foreign data wrappers.

## Parameters / Member Variables
- : NodeTag identifier for the structure type
- : Result relation's range table index, or 0 if not in range table
- : Relation descriptor for the result relation
- : Number of indices existing on the result relation
- : Array of relation descriptors for indices
- : Array of key/attribute info for indices
- : Attribute number of row identity junk attribute for UPDATE/DELETE
- : Attribute numbers of generated columns to compute for UPDATE
- : ProjectionInfo to generate new tuple in INSERT/UPDATE
- : Slot to hold the new tuple
- : Slot to hold the old tuple being updated
- : Whether projection and slots have been initialized
- : Whether updates need LockTuple() before reading old tuple
- : Triggers to be fired, if any
- : Cached lookup info for trigger functions
- : Array of trigger WHEN expression states
- : Optional runtime measurements for triggers
- , , : On-demand created slots for processing
- : FDW callback functions for foreign tables
- : Private state for FDW
- : Whether modifying foreign table directly
- , : Batch insert configuration and slots
- : List of WithCheckOption constraints
- : Array of constraint-checking expression states
- : Expression states for generated columns
- : List of RETURNING expressions
- : ProjectionInfo for computing RETURNING list
- : List of arbiter indexes for conflict checking
- : ON CONFLICT evaluation state
- : Lists of MergeActionState for MERGE operations
- : Expression state for MERGE join condition
- : Partition check expression state
- : Tuple conversion maps for partitioning
- : Target relation for tuple routing/transition capture
- : Tuple slot for partition routing
- : Buffer for COPY multi-inserts
- : List for cross-partition update handling

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag
  - [Relation](Relation.md)
  - RelationPtr
  - [IndexInfo](../I/IndexInfo.md)
  - [ProjectionInfo](../P/ProjectionInfo.md)
  - [TriggerDesc](../T/TriggerDesc.md)
  - [OnConflictSetState](../O/OnConflictSetState.md)
  - [TupleConversionMap](../T/TupleConversionMap.md)
  - [FdwRoutine](../F/FdwRoutine.md)
  - Various executor data types
- Called from (representative examples):
  - Data modification operations (INSERT, UPDATE, DELETE, MERGE)
  - [Trigger](../T/Trigger.md) execution
  - Partition handling
  - Foreign data wrapper operations

## Notes and Other Information
- Central to PostgreSQL's data modification pipeline
- Supports advanced features like partitioning, UPSERT, MERGE, and foreign tables
- Manages both regular and batch insert operations
- Handles complex constraint checking and trigger execution
- Essential for RETURNING clause processing
- Supports tuple conversion between parent and child partitions
- Integrates with foreign data wrapper architecture
- Critical for maintaining data integrity through constraint and trigger management