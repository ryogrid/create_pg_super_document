# PlanState

## Location
src/include/nodes/execnodes.h: 1113 - 1205

## Overview
PlanState serves as the abstract base class for all plan node execution states in PostgreSQL, providing common infrastructure for query plan execution including tuple processing, expression evaluation, and runtime instrumentation.

## Definition


## Detailed Description
PlanState is the fundamental abstract base class for all execution state structures in PostgreSQL's executor. It provides the common framework that all plan node types inherit, containing essential execution infrastructure such as tuple processing functions, instrumentation for performance monitoring, expression evaluation contexts, and tree navigation pointers. This structure forms the backbone of PostgreSQL's execution engine, with specific node types like SeqScanState, NestLoopState, and HashJoinState all extending this base structure.

## Parameters / Member Variables
- : NodeTag identifier for the specific PlanState subtype
- : Pointer to the associated Plan node from the plan tree
- : Global execution state (EState) shared across the entire query execution
- : Function pointer to retrieve the next tuple from this node
- : Actual processing function when ExecProcNode is a wrapper
- : Runtime performance statistics collection for this node
- : Per-worker performance statistics for parallel execution
- : Per-worker JIT compilation statistics
- : Expression state for boolean qualification conditions
- : Left child node in the execution tree
- : Right child node in the execution tree
- : List of uncorrelated subplans executed during initialization
- : List of correlated subplans referenced in expressions
- : Set of parameter IDs that have changed, triggering rescans
- : Descriptor for tuples returned by this node
- : Slot for storing result tuples
- : Expression evaluation context for this node
- : Projection information for tuple transformation
- : Flag indicating if node supports asynchronous execution
- : Tuple descriptor for scan slots (optimization hint)
- , , , : Slot operation types for different contexts
-  flags: Indicate whether corresponding slot types are guaranteed
-  flags: Indicate whether corresponding slot operation types are set

## Dependencies
- Functions called/Symbols referenced:
  - [Plan](Plan.md)
  - [EState](../E/EState.md)
  - ExecProcNodeMtd
  - Instrumentation
  - WorkerInstrumentation
  - [SharedJitInstrumentation](../S/SharedJitInstrumentation.md)
  - ExprState
  - [List](../L/List.md)
  - [Bitmapset](../B/Bitmapset.md)
  - [TupleDesc](../T/TupleDesc.md)
  - TupleTableSlot
  - ExprContext
  - [ProjectionInfo](ProjectionInfo.md)
  - TupleTableSlotOps
- Called from (representative examples):
  - All specific plan state structures (SeqScanState, NestLoopState, etc.)
  - [ExecInitNode](../E/ExecInitNode.md)
  - ExecProcNode
  - [ExecReScan](../E/ExecReScan.md)
  - [ExecEndNode](../E/ExecEndNode.md)

## Notes and Other Information
PlanState is never directly instantiated but serves as the common foundation for all executor node types. The structure includes sophisticated slot type management to optimize expression compilation and tuple processing. The async_capable flag enables PostgreSQL's asynchronous execution capabilities for improved parallelism. The instrumentation fields support PostgreSQL's query performance monitoring and EXPLAIN ANALYZE functionality. The dual function pointer design (ExecProcNode/ExecProcNodeReal) allows for wrapper functions that can add instrumentation or other cross-cutting concerns without affecting the core processing logic.