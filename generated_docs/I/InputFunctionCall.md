# InputFunctionCall

## Location
src/backend/utils/fmgr/fmgr.c: 1530 - 1584

## Overview
InputFunctionCall is a convenience wrapper that calls a previously-looked-up datatype input function to convert a string representation to its internal Datum format.

## Definition


## Detailed Description
This function provides a convenient interface for calling PostgreSQL's datatype input functions. It handles the setup of function call information and manages NULL value processing according to the function's strictness. The function takes a string representation of a value and converts it to the appropriate internal Datum representation using the specified input function. It includes error checking to ensure that NULL inputs produce NULL outputs and non-NULL inputs produce non-NULL outputs, maintaining data consistency.

## Parameters / Member Variables
- : Function manager info structure containing details about the input function to call
- : String representation of the value to convert (may be NULL to indicate a NULL value)
- : OID parameter passed to the input function (type-specific parameter)
- : Type modifier value providing additional type information

## Dependencies
- Functions called/Symbols referenced:
  - LOCAL_FCINFO (macro for local function call info)
  - InitFunctionCallInfoData (initializes function call structure)
  - CStringGetDatum (converts C string to Datum)
  - FunctionCallInvoke (invokes the actual function)
- Called from (representative examples):
  - BuildTupleFromCStrings
  - XmlTableGetValue
  - OidInputFunctionCall
  - plperl_sv_to_datum
  - PLyObject_ToScalar

## Notes and Other Information
- Handles NULL input specially: if str is NULL and the function is strict, returns NULL without calling the function
- Includes validation that NULL inputs produce NULL results and non-NULL inputs produce non-NULL results
- Part of PostgreSQL's function manager (fmgr) subsystem for type I/O operations
- Used extensively in procedural languages (PL/Perl, PL/Python, PL/Tcl) for data type conversions