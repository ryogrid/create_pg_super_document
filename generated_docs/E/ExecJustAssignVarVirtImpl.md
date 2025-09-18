# ExecJustAssignVarVirtImpl

## Location
[src/backend/executor/execExprInterp.c:2326-2347](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExprInterp.c#L2326-L2347)

## Overview
A core implementation function for efficiently assigning variables from virtual tuple slots to result slots in PostgreSQL's expression evaluation system.

## Definition


## Detailed Description
ExecJustAssignVarVirtImpl is the shared implementation function used by specialized variable assignment functions (ExecJustAssignInnerVarVirt, ExecJustAssignOuterVarVirt, and ExecJustAssignScanVarVirt). This function performs optimized variable assignment operations specifically for virtual tuple slots.

The function directly copies both the value and null indicator from a specified attribute in the input virtual slot to a specified position in the output result slot. This operation is highly optimized because virtual slots store values in readily accessible arrays, eliminating the need for tuple deforming or complex slot access patterns.

The function includes several assertions to verify that the operation is being performed under the expected conditions: the input slot must be virtual and fixed, and both the source attribute number and destination result number must be within valid ranges.

This function is part of PostgreSQL's expression evaluation optimization framework, where the expression compiler can generate more efficient code paths when it can guarantee certain slot types will be used.

## Parameters / Member Variables
- : ExprState containing the expression evaluation context and operation steps
- : Input TupleTableSlot (must be virtual) from which to read the variable value
- : Output parameter indicating if the assigned value is NULL (not used in return)

## Dependencies
- Functions called/Symbols referenced:
  - ExprEvalStep (for accessing operation details)
  - TTS_IS_VIRTUAL (macro for checking if slot is virtual)
  - TTS_FIXED (macro for checking if slot is fixed)
- Called from (representative examples):
  - [ExecJustAssignInnerVarVirt](ExecJustAssignInnerVarVirt.md)
  - [ExecJustAssignOuterVarVirt](ExecJustAssignOuterVarVirt.md)  
  - [ExecJustAssignScanVarVirt](ExecJustAssignScanVarVirt.md)

## Notes and Other Information
- This function is marked with pg_attribute_always_inline for maximum performance
- Always returns 0 (the return value is not meaningful for assignment operations)
- Performs direct array-to-array copying of values and null flags
- Includes comprehensive assertions to validate virtual slot constraints
- The operation details (source attnum and destination resultnum) are retrieved from the first evaluation step
- Uses state->resultslot as the destination for the assignment
- Part of a family of optimized assignment functions for different tuple contexts (inner, outer, scan)