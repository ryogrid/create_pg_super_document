# ExprEvalOp

## Location
[src/include/executor/execExpr.h:271-273](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/executor/execExpr.h#L271-L273)

## Overview
ExprEvalOp is an enumeration that serves as a discriminator for ExprEvalStep operations, identifying which specific operation should be executed during expression evaluation in PostgreSQL's expression interpreter.

## Definition


## Detailed Description
ExprEvalOp is a critical enumeration in PostgreSQL's expression evaluation system that defines all possible operations that can be performed during expression interpretation. It serves as the opcode discriminator for ExprEvalStep structures, which form the building blocks of compiled expressions in PostgreSQL's expression interpreter.

The enum is designed to work in conjunction with a dispatch table mechanism for efficient expression evaluation. Each enum value corresponds to a specific operation handler in the expression interpreter, allowing for fast, jump-table-based dispatch to the appropriate evaluation code.

The operations are categorized into several groups:
- **Variable access operations**: For fetching values from tuple slots (INNER, OUTER, SCAN variations)
- **Function evaluation**: Various optimized paths for function calls with different characteristics
- **Boolean logic**: AND, OR, NOT operations with special handling for short-circuiting
- **Control flow**: Jump operations for conditional evaluation
- **Type testing**: NULL tests, boolean tests, and type coercion
- **Complex expressions**: Array operations, row operations, field access, and aggregation
- **Special constructs**: Domain constraints, parameters, case expressions, and subplans

## Parameters / Member Variables
The enumeration values represent different operation types:

- : Marks completion of expression evaluation
- : Fetch multiple attributes from tuple slots efficiently
- : Access individual variable values from different tuple sources
- : Access system variables (like ctid, tableoid)
- : Fetch entire tuple as a composite value
- : Assign values to result slots
- : Evaluate constant values
- : Function call evaluation with various optimization flags
- : Boolean expression evaluation with short-circuiting
- : Control flow operations for conditional evaluation
- : NULL and boolean testing operations
- : Parameter evaluation (EXEC and EXTERN parameters)
- : Aggregation-specific operations
- : Sentinel value for array bounds checking

## Dependencies
- Functions called/Symbols referenced:
  - [ExprEvalStep](ExprEvalStep.md) (used as opcode discriminator)
  - dispatch_table (jump table in execExprInterp.c)

- Called from (representative examples):
  - [ExecComputeSlotInfo](ExecComputeSlotInfo.md) (src/backend/executor/execExpr.c:2902)
  - [ExprEvalOpLookup](ExprEvalOpLookup.md) (src/backend/executor/execExprInterp.c:108)
  - EEO_SWITCH (src/backend/executor/execExprInterp.c:124)
  - [ExecReadyInterpretedExpr](ExecReadyInterpretedExpr.md) (src/backend/executor/execExprInterp.c:278, 279, 327)
  - [ExecInitInterpreter](ExecInitInterpreter.md) (src/backend/executor/execExprInterp.c:2403, 2421)
  - [ExecEvalStepOp](ExecEvalStepOp.md) (src/backend/executor/execExprInterp.c:2440)
  - [llvm_compile_expr](../l/llvm_compile_expr.md) (src/backend/jit/llvm/llvmjit_expr.c:274)

## Notes and Other Information
- **Critical Ordering**: The order of enum entries must be kept in sync with the dispatch_table[] array in execExprInterp.c:ExecInterpExpr(). This synchronization is enforced by a static assertion.
- **Performance Optimization**: The enum is designed for high-performance expression evaluation using computed goto or jump table dispatch mechanisms.
- **JIT Integration**: The enum values are used by the LLVM JIT compiler to generate optimized native code for expression evaluation.
- **Extensibility**: New operation types can be added by extending the enum and implementing corresponding handlers in the interpreter.
- **Memory Layout**: The enum values are used as indices into dispatch tables, making the specific numeric values important for correct operation.
- **Debugging Support**: The reverse_dispatch_table allows mapping from opcode addresses back to enum values for debugging and introspection.