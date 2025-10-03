# SendFunctionCall

## Location
[src/backend/utils/fmgr/fmgr.c:1744-1753](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/fmgr.c#L1744-L1753)

## Overview
SendFunctionCall is a wrapper function that calls a previously-looked-up datatype binary-output function, guaranteeing a non-toasted result for data serialization.

## Definition

```c
bytea *
SendFunctionCall(FmgrInfo *flinfo, Datum val)
```
## Detailed Description
SendFunctionCall serves as a convenient wrapper around FunctionCall1 specifically for datatype binary-output functions. It takes a function manager info structure and a Datum value, then calls the underlying binary output function. The function guarantees that the returned bytea result is not toasted (compressed/stored externally), which is important for reliable data transmission and serialization. This function should not be called on NULL datums.

The function is part of PostgreSQL's function manager (fmgr) system, which handles dynamic function calls and provides a consistent interface for calling various types of functions including built-in and user-defined functions.

## Parameters / Member Variables
- `*flinfo`: Pointer to FmgrInfo structure containing cached information about the binary output function to call
- `val`: The Datum value to be converted to binary format (must not be NULL)
## Dependencies
- Functions called/Symbols referenced:
  - FunctionCall1
  - DatumGetByteaP
- Called from (representative examples):
  - [printtup](../p/printtup.md)
  - [CopyOneRowTo](../C/CopyOneRowTo.md)
  - [serializeAnalyzeReceive](../s/serializeAnalyzeReceive.md)
  - [array_send](../a/array_send.md)
  - [range_send](../r/range_send.md)
  - [record_send](../r/record_send.md)
  - [OidSendFunctionCall](../O/OidSendFunctionCall.md)

## Notes and Other Information
- This function must not be called with NULL datums
- The function ensures the result is not toasted, providing a guarantee that the underlying FunctionCall1 does not strictly provide
- It is primarily used in contexts where data needs to be serialized for transmission or storage in binary format
- The function is located in src/backend/utils/fmgr/fmgr.c at lines 1744-1753

## Simplified Source

```c
// Simplified version of SendFunctionCall
bytea *
SendFunctionCall(FmgrInfo *flinfo, Datum val)
{
    // Call the binary output function and ensure result is not toasted
    return DatumGetByteaP(FunctionCall1(flinfo, val));
}
```

Key simplifications made:
- Function is already very simple - it's essentially a one-line wrapper
- Added comment explaining the core purpose: calling binary output function with guaranteed non-toasted result
- The original function comment and logic are preserved since they're already minimal
- No complex error handling or branching to simplify - this is a straightforward wrapper function