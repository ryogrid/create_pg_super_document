# ExprState

## Location
src/include/nodes/execnodes.h: 78 - 141

## Overview
ExprState is the core runtime state structure for expression evaluation in PostgreSQL's executor, containing compiled instructions and storage for expression results.

## Definition


## Detailed Description
ExprState represents the compiled, executable form of an expression in PostgreSQL's executor. It transforms parse tree expressions into a sequence of evaluation steps that can be efficiently executed. The structure supports both scalar expression evaluation and tuple projection operations. The compilation process converts expression trees into a linear sequence of evaluation steps stored in the  array, which is then executed by the function pointer in . This design allows for optimized expression evaluation with minimal overhead during query execution.

## Parameters / Member Variables
- : Standard PostgreSQL node tag for type identification
- : Bitmask containing EEO_FLAG_* bits controlling evaluation behavior
- : Boolean flag indicating if the expression result is NULL
- : Datum storage for scalar expression results
- : TupleTableSlot pointer for tuple projection results, NULL for scalar expressions
- : Array of ExprEvalStep structures containing evaluation instructions
- : Function pointer to the actual evaluation function, optimized based on expression complexity
- : Pointer to original expression tree, retained for debugging purposes
- : Opaque pointer to private state data used by the evaluation function
- : Current number of evaluation steps in the steps array
- : Allocated capacity of the steps array
- : Pointer to parent PlanState node in the execution tree
- : ParamListInfo for resolving PARAM_EXTERN parameter nodes
- : Datum storage for CASE expression evaluation context
- : Null flag array for CASE expression evaluation
- : Datum storage for domain constraint checking
- : Null flag for domain constraint checking
- : Error handling context for soft error support during expression compilation

## Dependencies
- Functions called/Symbols referenced:
  - ExprEvalStep (evaluation instruction structure)
  - ParamListInfo (external parameter management)
  - ErrorSaveContext (soft error handling framework)
- Called from (representative examples):
  - ExecInitExpr (expression initialization)
  - ExecEvalExpr (expression evaluation)
  - ExecBuildProjectionInfo (projection setup)

## Notes and Other Information
- The structure is designed for high-performance expression evaluation with minimal runtime overhead
- Fields marked with FIELDNO_* macros are accessed by generated code and JIT compilation
- The steps array and related compilation fields could theoretically be discarded after compilation to save memory
- Supports both traditional interpretation and Just-In-Time (JIT) compilation for performance optimization
- The design allows for different evaluation strategies through the pluggable evalfunc mechanism