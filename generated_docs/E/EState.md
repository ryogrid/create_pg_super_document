# EState

## Location
[src/include/nodes/execnodes.h:621-728](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/execnodes.h#L621-L728)

## Overview
EState (Executor State) is the central working state structure for PostgreSQL's query executor, containing all the runtime information needed during query execution.

## Definition


## Detailed Description
EState serves as the comprehensive execution context for PostgreSQL queries, maintaining all runtime state needed during query execution. It bridges the gap between planning and execution by holding references to planned statements, managing runtime parameters, tracking tuple processing statistics, and coordinating resource management. The structure is designed to support complex execution scenarios including parallel execution, JIT compilation, triggers, and partitioning.

## Parameters / Member Variables
- : NodeTag identifier for the structure
- : Current scan direction (forward/backward)
- : Snapshot for visibility checks during execution
- : Snapshot for referential integrity crosschecks
- : List of range table entries from the query
- : Size of range table arrays
- : Array of opened Relation pointers indexed by range table
- : Array of row locking information per range table entry
- : List of permission information for range table entries
- : Reference to the top-level planned statement
- : Original SQL source text
- : Filter for removing junk attributes from result tuples
- : Command ID for marking inserted/deleted tuples
- : Array of target relation information for DML operations
- : List of opened result relations
- : Directory for partition descriptor lookups
- : Result relations created by tuple routing
- : Relations used only for trigger execution
- : External parameter values
- : Internal executor parameter values
- : Query environment for accessing named result sets
- : Memory context for per-query allocations
- : List of all TupleTableSlots used in execution
- : Number of tuples processed in current ExecutorRun call
- : Total tuples processed across all ExecutorRun calls
- : Execution flags passed to ExecutorStart
- : Instrumentation flags for performance monitoring
- : Flag indicating ExecutorFinish has completed
- : List of expression evaluation contexts
- : List of plan states for subplans
- : List of auxiliary ModifyTable states
- : Expression context for per-tuple operations
- : Active EPQ (EvalPlanQual) state for concurrent updates
- : Flag enabling parallel worker usage
- : Dynamic shared area for parallel execution coordination
- : JIT compilation control flags
- : JIT compilation context
- : Combined instrumentation from parallel workers
- : Relations with pending batch inserts
- : ModifyTable states for batch inserts

## Dependencies
- Functions called/Symbols referenced:
  - ScanDirection (enum type)
  - [ExecRowMark](ExecRowMark.md) (struct type)
  - [PlannedStmt](../P/PlannedStmt.md) (struct type)
  - [JunkFilter](../J/JunkFilter.md) (struct type)
  - CommandId (type)
  - [PartitionDirectory](../P/PartitionDirectory.md) (struct type)
  - [ParamListInfo](../P/ParamListInfo.md) (struct type)
  - [ParamExecData](../P/ParamExecData.md) (struct type)
  - QueryEnvironment (struct type)
  - [EPQState](EPQState.md) (struct type)
  - dsa_area (struct type)
  - [JitContext](../J/JitContext.md) (struct type)
  - [JitInstrumentation](../J/JitInstrumentation.md) (struct type)
- Called from (representative examples):
  - [ExecutorStart](ExecutorStart.md) (creates and initializes EState)
  - [ExecutorRun](ExecutorRun.md) (operates on EState)
  - [ExecutorFinish](ExecutorFinish.md) (finalizes EState)

## Notes and Other Information
EState is the cornerstone of PostgreSQL's execution engine, created once per query execution and passed to all executor nodes. It supports advanced features like parallel execution through shared memory areas, JIT compilation for performance optimization, and complex DML operations with partitioning and triggers. The structure's design allows for incremental tuple processing and maintains comprehensive statistics for monitoring and optimization purposes.