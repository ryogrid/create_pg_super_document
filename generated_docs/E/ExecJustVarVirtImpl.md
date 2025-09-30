# ExecJustVarVirtImpl

## Location
[src/backend/executor/execExprInterp.c:2283-2304](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExprInterp.c#L2283-L2304)

## Overview
ExecJustVarVirtImpl is a core implementation function for efficiently accessing variables from virtual tuple slots in PostgreSQL's expression evaluation system.

## Definition
```c
static pg_attribute_always_inline Datum ExecJustVarVirtImpl(ExprState *state, TupleTableSlot *slot, bool *isnull)
```

## Detailed Description
This function serves as the optimized implementation for variable access operations when the tuple is stored in a virtual slot. Virtual slots are a PostgreSQL optimization where tuple data is stored directly in memory arrays rather than in the traditional heap tuple format, avoiding the need for tuple deforming operations.

The function directly accesses the pre-computed values and null indicators from the virtual slot's arrays, making variable access extremely efficient. It includes several assertions to verify that the slot is indeed virtual and that the attribute number is valid, ensuring the optimization assumptions are correct.

## Parameters / Member Variables
- `state`: ExprState structure containing the expression evaluation steps and variable attribute information
- `slot`: TupleTableSlot containing the tuple data in virtual format
- `isnull`: Pointer to boolean flag that will be set to indicate if the variable value is NULL

## Dependencies
- Functions called/Symbols referenced:
  - [ExprEvalStep](ExprEvalStep.md) (expression evaluation step structure)
  - TTS_IS_VIRTUAL (macro to check if slot is virtual)
  - TTS_FIXED (macro to check if slot layout is fixed)
- Called from (representative examples):
  - [ExecJustInnerVarVirt](ExecJustInnerVarVirt.md) (wrapper for inner relation variables)
  - [ExecJustOuterVarVirt](ExecJustOuterVarVirt.md) (wrapper for outer relation variables)
  - [ExecJustScanVarVirt](ExecJustScanVarVirt.md) (wrapper for scan relation variables)

## Notes and Other Information
- This is a static function marked with pg_attribute_always_inline for maximum performance
- The function relies on the assumption that virtual slots never need tuple deforming operations
- Contains multiple assertions to verify the virtual slot assumptions at runtime
- Part of PostgreSQL's tuple slot optimization system that avoids expensive tuple deforming when possible
- Represents a key optimization in PostgreSQL's execution engine for handling projected columns efficiently

## Simplified Source

```c
static Datum ExecJustVarVirtImpl(ExprState *state, TupleTableSlot *slot, bool *isnull)
{
    ExprEvalStep *op = &state->steps[0];
    int attnum = op->d.var.attnum;

    // Virtual slots have pre-materialized values, direct access
    *isnull = slot->tts_isnull[attnum];
    return slot->tts_values[attnum];
}
```