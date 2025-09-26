# ExprEvalStep

## Location
src/include/executor/execExpr.h: 274 - 720

## Overview
ExprEvalStep represents a single instruction in PostgreSQL's compiled expression evaluation system, containing the operation to execute and its associated data in a cache-optimized structure.

## Definition


## Detailed Description
ExprEvalStep is the core building block of PostgreSQL's expression evaluation system. Each step represents a single atomic operation in the evaluation of a SQL expression, similar to instructions in a virtual machine. The structure is carefully designed to fit within 64 bytes (a single cache line on most systems) for optimal performance.

The opcode field initially contains an ExprEvalOp enum value during preparation, but can be modified later (e.g., replaced with function pointers for computed goto optimization). The resvalue and resnull fields point to where the step's result should be stored.

The large union 'd' contains operation-specific data for different types of steps, including variable fetches, function calls, boolean operations, array operations, JSON operations, aggregate functions, and many others. The union is constrained to 40 bytes on 64-bit systems to maintain the overall 64-byte step size.

## Parameters / Member Variables
- : Instruction identifier, initially an ExprEvalOp enum but may be modified for optimization (e.g., function pointers for computed goto)
- : Pointer to where the result Datum of this step should be stored
- : Pointer to where the null flag for this step's result should be stored
- : Union containing operation-specific inline data, limited to 40 bytes on 64-bit systems for cache efficiency

## Dependencies
- Functions called/Symbols referenced:
  - TupleTableSlotOps (for slot operations)
  - ExprEvalRowtypeCache (for composite type caching)
  - FunctionCallInfo (for function calls)
  - SubscriptingRefState (for array/subscript operations)
  - JsonConstructorExprState (for JSON construction)
  - WindowFuncExprState (for window functions)
  - SubPlanState (for subplans)
  - Various other PostgreSQL data types and structures
- Called from (representative examples):
  - ExecInitExprRec (expression initialization)
  - ExprEvalPushStep (step creation)
  - ExecInterpExpr (expression interpretation)
  - llvm_compile_expr (JIT compilation)

## Notes and Other Information
- The structure is carefully sized to fit in a single 64-byte cache line for optimal performance
- Different operation types use different members of the union, allowing efficient storage of diverse operation parameters
- The opcode field supports runtime optimization through replacement with computed goto targets
- Extensively used throughout PostgreSQL's expression evaluation system for all types of SQL expressions
- The design enables both interpreted and JIT-compiled execution of expressions
- Cache alignment and size constraints are critical for performance in high-frequency expression evaluation