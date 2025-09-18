# ExecuteQuery

## Location
[src/backend/commands/prepare.c:147-277](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/prepare.c#L147-L277)

## Overview
Implements the 'EXECUTE' utility statement and supports CREATE TABLE ... AS EXECUTE, executing a previously prepared statement with optional parameters and directing output to the specified destination.

## Definition


## Detailed Description
ExecuteQuery retrieves and executes a prepared statement by name, handling parameter evaluation, portal creation, and query execution. It supports both regular EXECUTE statements and CREATE TABLE ... AS EXECUTE constructs. The function validates that the prepared statement exists and has a fixed result type, evaluates any parameters using the current execution context, creates a portal for query execution, and runs the query through the portal interface. For CREATE TABLE ... AS EXECUTE, it performs additional validation to ensure the statement is a SELECT query.

## Parameters / Member Variables
- : Parse state containing parsing context information
- : ExecuteStmt node containing the prepared statement name and parameter values
- : IntoClause for CREATE TABLE ... AS EXECUTE (NULL for regular EXECUTE)
- : Parameter list information from outer query contexts
- : Destination receiver for query results
- : Query completion information structure

## Dependencies
- Functions called/Symbols referenced:
  - [FetchPreparedStatement](../F/FetchPreparedStatement.md) (retrieves prepared statement)
  - [CreateExecutorState](../C/CreateExecutorState.md) (creates execution state for parameter evaluation)
  - [EvaluateParams](EvaluateParams.md) (evaluates parameter expressions)
  - CreateNewPortal (creates portal for execution)
  - [GetCachedPlan](../G/GetCachedPlan.md) (gets cached plan for execution)
  - [PortalDefineQuery](../P/PortalDefineQuery.md) (defines query in portal)
  - [PortalStart](../P/PortalStart.md) (starts portal execution)
  - [PortalRun](../P/PortalRun.md) (runs portal to completion or specified count)
  - PortalDrop (cleans up portal)
  - [FreeExecutorState](../F/FreeExecutorState.md) (releases execution state)
- Called from (representative examples):
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md) (utility command processing)
  - [ExecCreateTableAs](ExecCreateTableAs.md) (CREATE TABLE AS execution)

## Notes and Other Information
- Validates that prepared statements have fixed result types to prevent variable-result plan execution
- Creates invisible portals for internal use that don't appear in pg_cursors
- Supports parameter evaluation with proper memory management to handle pass-by-reference parameters
- For CREATE TABLE ... AS EXECUTE, enforces that only SELECT statements are allowed
- Handles WITH NO DATA option for CREATE TABLE ... AS EXECUTE by setting fetch count to 0
- Manages plan reference counts carefully to prevent memory leaks between GetCachedPlan and PortalDefineQuery
- Uses portal interface for consistent query execution and resource management