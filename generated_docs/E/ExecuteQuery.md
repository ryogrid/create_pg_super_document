# ExecuteQuery

## Location
src/backend/commands/prepare.c: 147 - 277

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
  - FetchPreparedStatement (retrieves prepared statement)
  - CreateExecutorState (creates execution state for parameter evaluation)
  - EvaluateParams (evaluates parameter expressions)
  - CreateNewPortal (creates portal for execution)
  - GetCachedPlan (gets cached plan for execution)
  - PortalDefineQuery (defines query in portal)
  - PortalStart (starts portal execution)
  - PortalRun (runs portal to completion or specified count)
  - PortalDrop (cleans up portal)
  - FreeExecutorState (releases execution state)
- Called from (representative examples):
  - standard_ProcessUtility (utility command processing)
  - ExecCreateTableAs (CREATE TABLE AS execution)

## Notes and Other Information
- Validates that prepared statements have fixed result types to prevent variable-result plan execution
- Creates invisible portals for internal use that don't appear in pg_cursors
- Supports parameter evaluation with proper memory management to handle pass-by-reference parameters
- For CREATE TABLE ... AS EXECUTE, enforces that only SELECT statements are allowed
- Handles WITH NO DATA option for CREATE TABLE ... AS EXECUTE by setting fetch count to 0
- Manages plan reference counts carefully to prevent memory leaks between GetCachedPlan and PortalDefineQuery
- Uses portal interface for consistent query execution and resource management