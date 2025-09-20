# PlannedStmt

## Location
[src/include/nodes/plannodes.h:46-100](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/plannodes.h#L46-L100)

## Overview
PlannedStmt represents the output of PostgreSQL's planner, containing a Plan tree and all "one time" information needed by the executor to execute a query or utility statement.

## Definition

```c
typedef struct PlannedStmt
{
	pg_node_attr(no_equal, no_query_jumble)

	NodeTag		type;

	CmdType		commandType;	/* select|insert|update|delete|merge|utility */

	uint64		queryId;		/* query identifier (copied from Query) */

	bool		hasReturning;	/* is it insert|update|delete|merge RETURNING? */

	bool		hasModifyingCTE;	/* has insert|update|delete|merge in WITH? */

	bool		canSetTag;		/* do I set the command result tag? */

	bool		transientPlan;	/* redo plan when TransactionXmin changes? */

	bool		dependsOnRole;	/* is plan specific to current role? */

	bool		parallelModeNeeded; /* parallel mode required to execute? */

	int			jitFlags;		/* which forms of JIT should be performed */

	struct Plan *planTree;		/* tree of Plan nodes */

	List	   *rtable;			/* list of RangeTblEntry nodes */

	List	   *permInfos;		/* list of RTEPermissionInfo nodes for rtable
								 * entries needing one */

	/* rtable indexes of target relations for INSERT/UPDATE/DELETE/MERGE */
	List	   *resultRelations;	/* integer list of RT indexes, or NIL */

	List	   *appendRelations;	/* list of AppendRelInfo nodes */

	List	   *subplans;		/* Plan trees for SubPlan expressions; note
								 * that some could be NULL */

	Bitmapset  *rewindPlanIDs;	/* indices of subplans that require REWIND */

	List	   *rowMarks;		/* a list of PlanRowMark's */

	List	   *relationOids;	/* OIDs of relations the plan depends on */

	List	   *invalItems;		/* other dependencies, as PlanInvalItems */

	List	   *paramExecTypes; /* type OIDs for PARAM_EXEC Params */

	Node	   *utilityStmt;	/* non-null if this is utility stmt */

	/* statement location in source string (copied from Query) */
	ParseLoc	stmt_location;	/* start location, or -1 if unknown */
	ParseLoc	stmt_len;		/* length in bytes; 0 means "rest of string" */
} PlannedStmt;
```
## Detailed Description
PlannedStmt is the top-level node that wraps the execution plan tree produced by PostgreSQL's planner. It serves as a container for both the actual Plan tree and all auxiliary information required for execution. For utility statements (non-DML commands like CREATE TABLE, ALTER TABLE, etc.), the structure acts as a wrapper with the actual utility statement stored in the utilityStmt field and commandType set to CMD_UTILITY.

The structure includes execution metadata such as whether the statement returns data, requires parallel execution, needs JIT compilation, or depends on role-specific privileges. It also maintains dependency information for cache invalidation and parameter type information for prepared statements.

## Parameters / Member Variables
- `type`: Node tag identifying this as a PlannedStmt node
- `commandType`: Type of SQL command (SELECT, INSERT, UPDATE, DELETE, MERGE, or UTILITY)
- `queryId`: Unique identifier for the query, copied from the original Query node
- `hasReturning`: True if this is a data-modifying statement with a RETURNING clause
- `hasModifyingCTE`: True if the query contains data-modifying statements within WITH clauses
- `canSetTag`: True if this statement should set the command completion tag
- `transientPlan`: True if the plan needs to be regenerated when TransactionXmin changes
- `dependsOnRole`: True if the plan is specific to the current user role
- `parallelModeNeeded`: True if parallel execution mode is required
- `jitFlags`: Bitmask indicating which forms of JIT compilation should be applied
- `*planTree`: Root node of the execution plan tree
- `*rtable`: Range table containing all relations referenced in the query
- `*permInfos`: Permission information for relations that require access checks
- `*resultRelations`: List of range table indexes for target relations in DML operations
- `*appendRelations`: Information about inheritance and partitioning relationships
- `*subplans`: Plan trees for SubPlan expressions (subqueries, EXISTS, etc.)
- `*rewindPlanIDs`: Indexes of subplans that require rewind capability
- `*rowMarks`: List of row locking information for SELECT FOR UPDATE/SHARE
- `*relationOids`: OIDs of all relations the plan depends on (for cache invalidation)
- `*invalItems`: Additional dependency items for cache invalidation
- `*paramExecTypes`: Type information for PARAM_EXEC parameters
- `*utilityStmt`: The actual utility statement node (for utility commands)
- `stmt_location`: Starting character position of the statement in the source string
- `stmt_len`: Length of the statement in bytes
## Dependencies
- Functions called/Symbols referenced:
  - CmdType
  - ParseLoc
  - NodeTag
  - [Plan](Plan.md) (struct)
  - [List](../L/List.md)
  - [Bitmapset](../B/Bitmapset.md)
  - [Node](../N/Node.md)

- Called from (representative examples):
  - [planner](../p/planner.md) (optimizer/plan/planner.c:278,287)
  - [standard_planner](../s/standard_planner.md) (optimizer/plan/planner.c:291,538)
  - [CreateQueryDesc](../C/CreateQueryDesc.md) (tcop/pquery.c:67)
  - [ProcessQuery](ProcessQuery.md) (tcop/pquery.c:136)
  - [PortalStart](PortalStart.md) (tcop/pquery.c:495,547,569)
  - [InitPlan](../I/InitPlan.md) (executor/execMain.c:829)
  - ExecSerializePlan (executor/execParallel.c:147,174)

## Notes and Other Information
- [PlannedStmt](PlannedStmt.md) nodes do not support the equal() function, as there is currently no need for equality comparison of execution plans
- For utility statements, most fields are dummy values except canSetTag, stmt_location, stmt_len, and possibly queryId
- The structure is designed to contain all information needed for plan execution without requiring access to the original parse tree
- Cache invalidation relies heavily on the relationOids and invalItems fields to determine when plans need to be regenerated
- The transientPlan flag is used for plans that depend on transaction-specific state and may need regeneration within the same session