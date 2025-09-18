# OutputFunctionCall

## Location
src/backend/utils/fmgr/fmgr.c: 1683 - 1696

## Overview
OutputFunctionCall is a convenience wrapper that calls a previously-looked-up datatype output function to convert a Datum to its string representation.

## Definition
```c
char *OutputFunctionCall(FmgrInfo *flinfo, Datum val)
```

## Detailed Description
This function provides a simple interface for calling PostgreSQL's datatype output functions. It takes an internal Datum value and converts it to its external string representation using the specified output function. The function is essentially a thin wrapper around FunctionCall1 with the result converted from Datum to C string format. It's designed for non-NULL values and serves as the counterpart to InputFunctionCall for data type conversions.

## Parameters / Member Variables
- `flinfo`: Function manager info structure containing details about the output function to call
- `val`: The Datum value to convert to string representation (must not be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - FunctionCall1 (calls function with one argument)
  - [DatumGetCString](../D/DatumGetCString.md) (converts Datum result to C string)
- Called from (representative examples):
  - [printtup](../p/printtup.md) (tuple output)
  - [CopyOneRowTo](../C/CopyOneRowTo.md) (COPY command output)
  - [array_out](../a/array_out.md) (array output function)
  - [record_out](../r/record_out.md) (record output function)
  - [text_format](../t/text_format.md) (format string processing)
  - [OidOutputFunctionCall](OidOutputFunctionCall.md)

## Notes and Other Information
- Should not be called on NULL datums - caller must handle NULL values separately
- Currently implemented as simple window dressing for FunctionCall1
- Used extensively throughout PostgreSQL for converting internal values to external string format
- Essential component of PostgreSQL's type system for data export and display
- Used in COPY operations, tuple printing, array/record serialization, and procedural language interfaces
- Part of the core function manager (fmgr) infrastructure for type I/O operations