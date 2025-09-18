# ExecCreateTableAs

## Location
src/backend/commands/createas.c: 221 - 367

## Overview
Main entry point function for executing CREATE TABLE AS and CREATE MATERIALIZED VIEW statements, handling the complete workflow from query planning to relation creation and data population.

## Definition


## Detailed Description
The  function serves as the primary execution handler for both CREATE TABLE AS SELECT and CREATE MATERIALIZED VIEW statements. It orchestrates the entire process including relation existence checking, query rewriting, planning, execution, and data population.

The function handles different execution paths based on the statement type and options:
- For EXECUTE statements within CREATE TABLE AS, it delegates to ExecuteQuery
- For WITH NO DATA scenarios, it uses create_ctas_nodata for efficient relation creation without query execution
- For regular CREATE TABLE AS, it performs complete query rewriting, planning, and execution
- For materialized views, it creates the relation with skipData=true initially, then uses REFRESH logic for secure data population

The function manages snapshot handling, query descriptor creation, executor lifecycle, and proper cleanup. For materialized views, it implements a two-phase approach to leverage the security restrictions of the REFRESH operation.

## Parameters / Member Variables
- : ParseState containing parsing context and source text information
- : CreateTableAsStmt node containing the parsed statement structure
- : ParamListInfo for handling parameterized queries
- : QueryEnvironment for query execution context
- : QueryCompletion structure for returning execution statistics

## Dependencies
- Functions called/Symbols referenced:
  - CreateTableAsRelExists
  - CreateIntoRelDestReceiver
  - ExecuteQuery
  - create_ctas_nodata
  - QueryRewrite
  - pg_plan_query
  - PushCopiedSnapshot
  - GetActiveSnapshot
  - UpdateActiveSnapshotCommandId
  - CreateQueryDesc
  - GetIntoRelEFlags
  - ExecutorStart
  - ExecutorRun
  - ExecutorFinish
  - ExecutorEnd
  - RefreshMatViewByOid
- Called from (representative examples):
  - ProcessUtilitySlow

## Notes and Other Information
- This is a public function exposed through the createas.h header
- Implements sophisticated execution flow control based on statement characteristics
- Handles both regular tables and materialized views with different strategies
- Uses snapshot management to ensure consistency during query execution
- For materialized views, leverages REFRESH logic to maintain security restrictions
- Properly manages executor lifecycle with appropriate startup, run, finish, and end phases
- Returns ObjectAddress for the created relation for further processing by callers
- Implements error handling for unexpected rewrite results and validation scenarios