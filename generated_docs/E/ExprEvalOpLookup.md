# ExprEvalOpLookup

## Location
src/backend/executor/execExprInterp.c: 105 - 109

## Overview
ExprEvalOpLookup is a simple structure used to map jump target opcodes back to their corresponding ExprEvalOp enumeration values in PostgreSQL's expression evaluation system when using computed goto (direct threading) optimization.

## Definition


## Detailed Description
This structure serves as a lookup table entry for reverse mapping in PostgreSQL's expression interpreter. When direct threading is enabled (EEO_USE_COMPUTED_GOTO), the expression evaluator uses computed goto labels as opcodes for performance optimization. However, this makes it difficult to determine the original ExprEvalOp enumeration value from a given opcode address. ExprEvalOpLookup provides the mapping needed to convert these opcode addresses back to their corresponding ExprEvalOp values.

The structure is primarily used in conjunction with bsearch() to quickly find the ExprEvalOp value corresponding to a given opcode address in the reverse_dispatch_table array.

## Parameters / Member Variables
- : A void pointer containing the computed goto label address used as the opcode in direct-threaded mode
- : The corresponding ExprEvalOp enumeration value that represents the actual operation type

## Dependencies
- Functions called/Symbols referenced:
  - ExprEvalOp (enumeration type for expression evaluation operations)
- Called from (representative examples):
  - dispatch_compare_ptr (used as comparison function parameter type)
  - ExecEvalStepOp (creates instances for bsearch lookups)
  - ExecInitInterpreter (used in building reverse dispatch table)

## Notes and Other Information
- This structure is only relevant when EEO_USE_COMPUTED_GOTO is defined and direct threading is enabled
- Used as the element type in the reverse_dispatch_table array for efficient opcode-to-operation mapping
- The lookup is performed using binary search (bsearch) with dispatch_compare_ptr as the comparator
- Essential for debugging and introspection capabilities in the expression evaluation system when using performance optimizations