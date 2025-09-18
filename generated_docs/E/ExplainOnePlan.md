# ExplainOnePlan

## Location
src/backend/commands/explain.c: 617 - 806

## Overview
ExplainOnePlan executes a planned query (if analysis is requested) and generates comprehensive EXPLAIN output including execution statistics, buffer usage, memory counters, and timing information.

## Definition


## Detailed Description
ExplainOnePlan is the core function responsible for executing planned queries and generating detailed EXPLAIN output. It handles both EXPLAIN (plan-only) and EXPLAIN ANALYZE (with execution) scenarios.

The function performs several key operations:
1. Sets up instrumentation options based on the requested explain options (timing, buffers, WAL)
2. Creates appropriate destination receivers for different scenarios (INTO clauses, serialization)
3. Executes the query if ANALYZE is requested, collecting runtime statistics
4. Generates comprehensive output including plan structure, execution statistics, buffer usage, memory usage, JIT information, trigger statistics, and serialization metrics
5. Manages snapshots and command counter increments for proper transaction handling

The function is exported for use by prepare.c in EXPLAIN EXECUTE scenarios and for potential index advisor plugins.

## Parameters / Member Variables
- : The planned statement to execute and explain
- : IntoClause for CREATE TABLE AS statements, NULL otherwise  
- : ExplainState containing output formatting options and state
- : Original query string for context and error messages
- : Parameter list for parameterized queries
- : Query environment providing additional context
- : Planning time duration for summary reporting
- : Buffer usage statistics from planning phase
- : Memory usage counters from planning phase

## Dependencies
- Functions called/Symbols referenced:
  - CreateQueryDesc
  - ExecutorStart
  - ExecutorRun  
  - ExecutorFinish
  - ExecutorEnd
  - ExplainPrintPlan
  - ExplainPrintTriggers
  - ExplainPrintJITSummary
  - ExplainPrintSerialize
  - CreateIntoRelDestReceiver
  - CreateExplainSerializeDestReceiver
  - PushCopiedSnapshot
  - GetActiveSnapshot
  - UpdateActiveSnapshotCommandId
  - elapsed_time
- Called from (representative examples):
  - standard_ExplainOneQuery
  - ExplainExecuteQuery

## Notes and Other Information
- The function always collects timing for the entire statement regardless of node-level timing settings
- For CREATE TABLE AS with ANALYZE, special handling ensures proper data flow to the destination table
- EXPLAIN ANALYZE CREATE TABLE AS WITH NO DATA uses NoMovementScanDirection to avoid data creation
- JIT information is tied to the costs option to avoid regression test output differences
- Serialization metrics are captured before destroying the destination receiver
- The function handles snapshot management to ensure proper visibility of previously executed statements
- Memory and buffer usage from both planning and execution phases are reported when available