# ModifyTable

## Location
[src/include/nodes/plannodes.h:229-256](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/plannodes.h#L229-L256)

## Overview
ModifyTable is a plan node that applies data modification operations (INSERT, UPDATE, DELETE, MERGE) to target tables using rows produced by its outer plan, supporting complex features like partitioning, inheritance, triggers, and conflict resolution.

## Definition

```c
typedef struct ModifyTable
{
	Plan		plan;
	CmdType		operation;		/* INSERT, UPDATE, DELETE, or MERGE */
	bool		canSetTag;		/* do we set the command tag/es_processed? */
	Index		nominalRelation;	/* Parent RT index for use of EXPLAIN */
	Index		rootRelation;	/* Root RT index, if partitioned/inherited */
	bool		partColsUpdated;	/* some part key in hierarchy updated? */
	List	   *resultRelations;	/* integer list of RT indexes */
	List	   *updateColnosLists;	/* per-target-table update_colnos lists */
	List	   *withCheckOptionLists;	/* per-target-table WCO lists */
	List	   *returningLists; /* per-target-table RETURNING tlists */
	List	   *fdwPrivLists;	/* per-target-table FDW private data lists */
	Bitmapset  *fdwDirectModifyPlans;	/* indices of FDW DM plans */
	List	   *rowMarks;		/* PlanRowMarks (non-locking only) */
	int			epqParam;		/* ID of Param for EvalPlanQual re-eval */
	OnConflictAction onConflictAction;	/* ON CONFLICT action */
	List	   *arbiterIndexes; /* List of ON CONFLICT arbiter index OIDs  */
	List	   *onConflictSet;	/* INSERT ON CONFLICT DO UPDATE targetlist */
	List	   *onConflictCols; /* target column numbers for onConflictSet */
	Node	   *onConflictWhere;	/* WHERE for ON CONFLICT UPDATE */
	Index		exclRelRTI;		/* RTI of the EXCLUDED pseudo relation */
	List	   *exclRelTlist;	/* tlist of the EXCLUDED pseudo relation */
	List	   *mergeActionLists;	/* per-target-table lists of actions for
									 * MERGE */
	List	   *mergeJoinConditions;	/* per-target-table join conditions
										 * for MERGE */
} ModifyTable;
```
## Detailed Description
ModifyTable is the primary plan node for executing data modification operations in PostgreSQL. It handles the complex orchestration of INSERT, UPDATE, DELETE, and MERGE operations, including support for advanced features like table partitioning, inheritance hierarchies, foreign data wrappers, row-level security, triggers, and conflict resolution.

The node processes rows from its outer plan and applies the specified operation to one or more target tables. For partitioned tables or inheritance hierarchies, it manages the routing of rows to appropriate child tables. It coordinates with the trigger system, handles RETURNING clauses, manages foreign key constraints, and implements PostgreSQL's sophisticated conflict resolution mechanisms.

For MERGE operations, it maintains action lists and join conditions for each target table. For INSERT operations with ON CONFLICT clauses, it manages conflict detection and resolution. The node also integrates with PostgreSQL's concurrency control through EvalPlanQual (EPQ) mechanisms.

## Parameters / Member Variables
- : Base Plan structure with common execution plan fields
- : Type of modification operation (INSERT, UPDATE, DELETE, MERGE)
- : Whether this operation sets the command completion tag
- : Range table index of the relation shown in EXPLAIN output
- : Range table index of root relation for partitioned/inherited tables (0 if not applicable)
- : True if any partitioning key columns are being updated
- : List of range table indexes for all target relations
- : Per-target-table lists of column numbers being updated
- : Per-target-table WITH CHECK OPTION constraint lists
- : Per-target-table RETURNING clause target lists
- : Per-target-table private data for foreign data wrapper operations
- : Bitmap indicating which plans use FDW direct modification
- : Row locking information (for non-locking row marks)
- : Parameter ID for EvalPlanQual re-evaluation during concurrent updates
- : Action to take on INSERT conflicts (IGNORE or UPDATE)
- : List of index OIDs used for ON CONFLICT conflict detection
- : Target list for ON CONFLICT DO UPDATE operations
- : Column numbers targeted by ON CONFLICT DO UPDATE
- : WHERE clause for ON CONFLICT DO UPDATE operations
- : Range table index of the EXCLUDED pseudo-relation for ON CONFLICT
- : Target list of the EXCLUDED pseudo-relation
- : Per-target-table action specifications for MERGE operations
- : Per-target-table join conditions for MERGE operations

## Dependencies
- Functions called/Symbols referenced:
  - [Plan](../P/Plan.md) (base structure)
  - CmdType
  - OnConflictAction
  - Index
  - [List](../L/List.md)
  - [Bitmapset](../B/Bitmapset.md)
  - [Node](../N/Node.md)

- Called from (representative examples):
  - make_modifytable (optimizer/plan/createplan.c:7040)
  - [create_modifytable_plan](../c/create_modifytable_plan.md) (optimizer/plan/createplan.c:2817)
  - [ExecInitModifyTable](../E/ExecInitModifyTable.md) (executor/nodeModifyTable.c:4422)
  - [ExecInsert](../E/ExecInsert.md) (executor/nodeModifyTable.c:793)
  - [ExecInitMerge](../E/ExecInitMerge.md) (executor/nodeModifyTable.c:3486)

## Notes and Other Information
- Central node for all data modification operations in PostgreSQL's execution engine
- Handles complex multi-table scenarios including partitioned tables and inheritance hierarchies
- Integrates with PostgreSQL's comprehensive trigger system for BEFORE/AFTER/INSTEAD OF triggers
- Supports sophisticated conflict resolution through ON CONFLICT clauses with IGNORE and UPDATE actions
- Manages RETURNING clauses that can return modified rows to the client
- Coordinates with foreign data wrappers for modifications on remote tables
- Implements EvalPlanQual mechanisms for handling concurrent modifications
- Critical for MERGE statement execution, which combines INSERT, UPDATE, and DELETE logic
- Row-level security policies are enforced through WITH CHECK OPTION mechanisms
- The complexity of this node reflects PostgreSQL's rich feature set for data modification operations