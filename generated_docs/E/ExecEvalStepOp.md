# ExecEvalStepOp

## Location
src/backend/executor/execExprInterp.c: 2422 - 2451

## Overview
Function that returns the appropriate ExprEvalOp enumeration value for an expression step, handling the complexity of direct-threaded dispatch where opcodes are stored as jump addresses.

## Definition
```c
ExprEvalOp ExecEvalStepOp(ExprState *state, ExprEvalStep *op)
```

## Detailed Description
This function serves as a bridge between the optimized direct-threaded execution model and code that needs to inspect the actual opcode of an expression step. When direct threading is enabled (EEO_FLAG_DIRECT_THREADED), the opcode field contains a jump target address rather than the symbolic opcode enum. In such cases, this function performs a binary search in the reverse_dispatch_table to find the corresponding ExprEvalOp enum value. For non-direct-threaded execution, it simply casts the opcode field to the enum type.

## Parameters / Member Variables
- `state`: Pointer to the ExprState containing expression execution state and flags
- `op`: Pointer to the ExprEvalStep whose opcode needs to be determined

## Dependencies
- Functions called/Symbols referenced:
  - bsearch (for binary search in reverse lookup table)
  - dispatch_compare_ptr (comparator function for binary search)
  - EEO_FLAG_DIRECT_THREADED (flag constant)
  - EEOP_LAST (constant for array size)
  - ExprEvalOpLookup (structure type)
  - ExprEvalOp (enumeration type)
- Called from (representative examples):
  - CheckExprStillValid (at line 1949)
  - llvm_compile_expr (in LLVM JIT compilation)

## Notes and Other Information
- Essential for debugging and introspection when direct threading is active
- Uses the reverse_dispatch_table built by ExecInitInterpreter
- Only compiled when EEO_USE_COMPUTED_GOTO is defined
- Includes assertion to catch lookups of unknown opcodes
- Returns ExprEvalOp enum value that can be used for switch statements or debugging