# PlannedStmt

## Location
src/include/nodes/plannodes.h: 46 - 100

## Overview
PlannedStmt represents the output of PostgreSQL's planner, containing a Plan tree and all "one time" information needed by the executor to execute a query or utility statement.

## Definition


## Detailed Description
PlannedStmt is the top-level node that wraps the execution plan tree produced by PostgreSQL's planner. It serves as a container for both the actual Plan tree and all auxiliary information required for execution. For utility statements (non-DML commands like CREATE TABLE, ALTER TABLE, etc.), the structure acts as a wrapper with the actual utility statement stored in the utilityStmt field and commandType set to CMD_UTILITY.

The structure includes execution metadata such as whether the statement returns data, requires parallel execution, needs JIT compilation, or depends on role-specific privileges. It also maintains dependency information for cache invalidation and parameter type information for prepared statements.

## Parameters / Member Variables
- : Node tag identifying this as a PlannedStmt node
- : Type of SQL command (SELECT, INSERT, UPDATE, DELETE, MERGE, or UTILITY)
- : Unique identifier for the query, copied from the original Query node
- : True if this is a data-modifying statement with a RETURNING clause
- : True if the query contains data-modifying statements within WITH clauses
- : True if this statement should set the command completion tag
- : True if the plan needs to be regenerated when TransactionXmin changes
- : True if the plan is specific to the current user role
- : True if parallel execution mode is required
- : Bitmask indicating which forms of JIT compilation should be applied
- : Root node of the execution plan tree
- : Range table containing all relations referenced in the query
- : Permission information for relations that require access checks
- : List of range table indexes for target relations in DML operations
- : Information about inheritance and partitioning relationships
- : Plan trees for SubPlan expressions (subqueries, EXISTS, etc.)
- : Indexes of subplans that require rewind capability
- : List of row locking information for SELECT FOR UPDATE/SHARE
- : OIDs of all relations the plan depends on (for cache invalidation)
- : Additional dependency items for cache invalidation
- : Type information for PARAM_EXEC parameters
- : The actual utility statement node (for utility commands)
- : Starting character position of the statement in the source string
- : Length of the statement in bytes

## Dependencies
- Functions called/Symbols referenced:
  - CmdType
  - ParseLoc
  - NodeTag
  - Plan (struct)
  - List
  - Bitmapset
  - Node

- Called from (representative examples):
  - planner (optimizer/plan/planner.c:278,287)
  - standard_planner (optimizer/plan/planner.c:291,538)
  - CreateQueryDesc (tcop/pquery.c:67)
  - ProcessQuery (tcop/pquery.c:136)
  - PortalStart (tcop/pquery.c:495,547,569)
  - InitPlan (executor/execMain.c:829)
  - ExecSerializePlan (executor/execParallel.c:147,174)

## Notes and Other Information
- PlannedStmt nodes do not support the equal() function, as there is currently no need for equality comparison of execution plans
- For utility statements, most fields are dummy values except canSetTag, stmt_location, stmt_len, and possibly queryId
- The structure is designed to contain all information needed for plan execution without requiring access to the original parse tree
- Cache invalidation relies heavily on the relationOids and invalItems fields to determine when plans need to be regenerated
- The transientPlan flag is used for plans that depend on transaction-specific state and may need regeneration within the same session