# CheckExprStillValid

## Location
src/backend/executor/execExprInterp.c: 1935 - 1985

## Overview
Validates that variable references in an expression remain compatible with the current schema by checking each variable operation against its corresponding tuple slot.

## Definition
```c
void CheckExprStillValid(ExprState *state, ExprContext *econtext)
```

## Detailed Description
This function performs schema compatibility validation for compiled expressions to handle potential schema changes that occurred after the expression was initially compiled. It iterates through all expression evaluation steps and validates variable references against the current tuple descriptors.

The function specifically checks three types of variable operations:
- **EEOP_INNER_VAR**: Variables from the inner tuple slot (joins)
- **EEOP_OUTER_VAR**: Variables from the outer tuple slot (joins)  
- **EEOP_SCAN_VAR**: Variables from the scan tuple slot (base table scans)

For each variable operation encountered, it:
1. Extracts the attribute number and expected data type from the operation
2. Calls CheckVarSlotCompatibility() to verify the attribute exists and has the expected type
3. Throws an error if incompatibilities are detected (handled in CheckVarSlotCompatibility)

This validation is essential for handling scenarios such as:
- DDL operations that add/drop/modify columns
- Table replacements with different schemas
- View definition changes
- Cached plan reuse across schema modifications

The function only checks variable operations because these are the primary points where schema changes can cause runtime failures. Other operation types (constants, functions, etc.) are generally schema-independent.

## Parameters / Member Variables
- `state`: Pointer to ExprState containing the expression steps to validate
- `econtext`: Expression context providing access to inner, outer, and scan tuple slots

## Dependencies
- Functions called/Symbols referenced:
  - ExecEvalStepOp (extracts opcode from step, handling computed goto conversion)
  - CheckVarSlotCompatibility (validates individual variable-slot compatibility)
- Called from:
  - ExecInterpExprStillValid (one-time validation wrapper)
  - ExecRunCompiledExpr (JIT-compiled expression validation)

## Notes and Other Information
- Only validates variable operations (EEOP_*_VAR), ignoring other operation types that are schema-independent
- Uses ExecEvalStepOp() to properly extract opcodes, which is important when computed goto direct threading is enabled
- The validation is performed once per expression state lifetime, not on every execution
- Critical for PostgreSQL's ability to handle schema evolution gracefully in long-running sessions
- Errors are handled by CheckVarSlotCompatibility, which will throw appropriate exceptions for schema mismatches
- Part of PostgreSQL's defensive programming strategy to catch schema-related issues at runtime rather than causing crashes or data corruption