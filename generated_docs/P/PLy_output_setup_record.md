# PLy_output_setup_record

## Location
src/pl/plpython/plpy_typeio.c: 261 - 295

## Overview
PLy_output_setup_record sets up output conversion information for PL/Python functions that return a RECORD type, ensuring the tuple descriptor is properly blessed and configured for composite type output.

## Definition


## Detailed Description
This function is responsible for configuring the output conversion infrastructure when a PL/Python function returns a RECORD type. It performs several critical tasks:

1. **Type validation**: Ensures that both the argument and tuple descriptor are of RECORD type
2. **Tuple descriptor blessing**: Calls BlessTupleDesc to finalize the tuple descriptor for use with Set-Returning Functions (SRFs)
3. **Type modifier management**: Updates the typmod and clears any cached record descriptor if the type modifier has changed
4. **Delegation**: Calls PLy_output_setup_tuple to handle the actual tuple setup

The function assumes that the tuple descriptor may not be long-lived and takes appropriate measures to ensure proper type information is maintained for subsequent composite type conversions.

## Parameters / Member Variables
- : PLyObToDatum structure containing output conversion information to be configured
- : TupleDesc structure representing the record type's tuple descriptor (may be transient)
- : PLyProcedure structure containing procedure metadata (passed through to subsequent setup)

## Dependencies
- Functions called/Symbols referenced:
  - [BlessTupleDesc](../B/BlessTupleDesc.md): Finalizes tuple descriptor for SRF use
  - [PLy_output_setup_tuple](PLy_output_setup_tuple.md): Handles the actual tuple conversion setup
- Referenced types:
  - [PLyObToDatum](PLyObToDatum.md): Output conversion structure
  - [PLyProcedure](PLyProcedure.md): Procedure metadata structure
- Called from:
  - [PLy_exec_function](PLy_exec_function.md): Main function execution path
  - [PLyObToDatum](PLyObToDatum.md): Output conversion initialization

## Notes and Other Information
- This function is specific to RECORD types and will assert if called with other types
- The function handles the case where tuple descriptors may be transient by ensuring proper blessing
- Type modifier changes are detected and handled by clearing cached record descriptors
- Located in src/pl/plpython/plpy_typeio.c at lines 261-295