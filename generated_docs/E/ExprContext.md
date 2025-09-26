# ExprContext

## Location
src/include/nodes/execnodes.h: 251 - 297

## Overview
ExprContext holds the current execution context information needed for evaluating expressions during tuple qualification and projection operations in PostgreSQL's executor.

## Definition


## Detailed Description
ExprContext serves as the runtime environment for expression evaluation in PostgreSQL's executor. It provides access to current tuples from different sources (scan, inner, outer), manages memory allocation for expression evaluation, and maintains parameter substitution values. The context supports complex query operations including joins (through inner/outer tuple access), aggregate functions (through precomputed aggregate values), CASE expressions, and domain constraint checking. Two distinct memory contexts are maintained: per-query memory for persistent data like function call caches, and per-tuple memory that is reset for each tuple evaluation to prevent memory leaks.

## Parameters / Member Variables
- : Standard PostgreSQL node tag for type identification
- : TupleTableSlot containing the current tuple being scanned by this node
- : TupleTableSlot containing the current inner tuple for join operations
- : TupleTableSlot containing the current outer tuple for join operations
- : Long-term memory context with query lifespan, used for function call caches
- : Short-term memory context reset per tuple, used for expression results
- : Array of ParamExecData for PARAM_EXEC parameter substitution
- : ParamListInfo for other parameter types (external, user-defined)
- : Array of precomputed Datum values for aggregate and window function nodes
- : Array of null flags corresponding to aggregate and window function values
- : Datum value to substitute for CaseTestExpr nodes in CASE expressions
- : Null flag for the CASE test expression value
- : Datum value for CoerceToDomainValue nodes in domain constraint checking
- : Null flag for domain constraint value
- : Pointer to the containing executor state, NULL for standalone contexts
- : Linked list of cleanup callback functions to execute on context shutdown

## Dependencies
- Functions called/Symbols referenced:
  - ParamExecData (executor parameter data structure)
  - ParamListInfo (parameter list management)
  - ExprContext_CB (cleanup callback structure)
- Called from (representative examples):
  - ExecEvalExpr (expression evaluation entry point)
  - ExecInitExprContext (context initialization)
  - ExecProject (tuple projection operations)

## Notes and Other Information
- CurrentMemoryContext should be set to ecxt_per_tuple_memory before calling ExecEvalExpr()
- The per-tuple memory context is typically reset once per tuple to prevent memory accumulation
- Fields marked with FIELDNO_* macros are accessed directly by generated code and JIT compilation
- The context supports nested expression evaluation through the callback mechanism
- Aggregate and window function values are precomputed and stored for efficient access during expression evaluation
- The design allows expression evaluation to access multiple tuple sources simultaneously, essential for join operations