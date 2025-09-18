# ExecJustAssignVarImpl

## Location
[src/backend/executor/execExprInterp.c:2188-2213](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExprInterp.c#L2188-L2213)

## Overview
ExecJustAssignVarImpl is the core implementation function for fast-path variable assignment operations in PostgreSQL's expression evaluation system, handling the assignment of variable values to result tuple slots.

## Definition
```c
static pg_attribute_always_inline Datum ExecJustAssignVarImpl(ExprState *state, TupleTableSlot *inslot, bool *isnull)
```

## Detailed Description
ExecJustAssignVarImpl serves as the shared implementation for the ExecJustAssign*Var family of functions (ExecJustAssignInnerVar, ExecJustAssignOuterVar, and ExecJustAssignScanVar). This function optimizes the common case of simple variable assignments where a value from an input tuple slot needs to be copied to a specific position in the result tuple slot.

The function extracts a value from the specified attribute of the input slot and assigns it to the designated position in the output slot. It's part of PostgreSQL's expression evaluation fast-path optimization system, designed to avoid the overhead of the general expression interpreter for simple assignment operations.

Unlike the ExecJustVar* functions that return the extracted value, this function performs assignment and always returns 0, since the actual result is stored in the output slot.

## Parameters / Member Variables
- `state`: ExprState containing expression evaluation state and step information including result slot
- `inslot`: Input TupleTableSlot from which to extract the variable value
- `isnull`: Output parameter indicating whether the extracted value is NULL (though not directly used in this implementation)

## Dependencies
- Functions called/Symbols referenced:
  - ExprEvalStep (step structure access)
  - [CheckOpSlotCompatibility](../C/CheckOpSlotCompatibility.md) (input slot validation)
  - slot_getattr (attribute extraction)
- Called from (representative examples):
  - [ExecJustAssignInnerVar](ExecJustAssignInnerVar.md)
  - [ExecJustAssignOuterVar](ExecJustAssignOuterVar.md)  
  - [ExecJustAssignScanVar](ExecJustAssignScanVar.md)

## Notes and Other Information
- Always marked as pg_attribute_always_inline for maximum performance
- Returns 0 as the actual result is stored in the output slot's tts_values array
- Includes runtime assertion to validate that resultnum is within the valid range of output slot attributes
- Uses CheckOpSlotCompatibility for input validation but relies on compile-time checks for output slot compatibility
- Part of the assignment expression fast-path that complements the variable reference fast-path functions
- The function directly manipulates the output slot's tts_values and tts_isnull arrays for efficiency