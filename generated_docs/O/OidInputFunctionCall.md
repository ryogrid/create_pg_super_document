# OidInputFunctionCall

## Location
[src/backend/utils/fmgr/fmgr.c:1754-1762](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/fmgr.c#L1754-L1762)

## Overview
OidInputFunctionCall is a convenience function that calls a datatype input function identified by its OID, intended for seldom-executed code paths due to performance and memory considerations.

## Definition


## Detailed Description
OidInputFunctionCall provides a simple interface for calling datatype input functions when you only have the function's OID rather than a pre-cached FmgrInfo structure. The function internally sets up the function manager info using fmgr_info() and then calls InputFunctionCall() to perform the actual conversion.

This function is designed for infrequently executed code paths because it has performance overhead (function lookup on each call) and memory leakage issues since it doesn't cache the function information. For frequently called code, it's better to pre-cache the FmgrInfo structure and use InputFunctionCall directly.

The function is part of PostgreSQL's type input/output system, converting string representations of data into internal Datum format according to the specified datatype's input function.

## Parameters / Member Variables
- : OID of the input function to call for the datatype conversion
- : String representation of the value to be converted to internal format
- : Type-specific parameter passed to the input function (often the element type OID for container types)
- : Type modifier value providing additional type-specific information

## Dependencies
- Functions called/Symbols referenced:
  - [fmgr_info](../f/fmgr_info.md)
  - [InputFunctionCall](../I/InputFunctionCall.md)
- Called from (representative examples):
  - [InsertOneValue](../I/InsertOneValue.md)
  - [DefineAggregate](../D/DefineAggregate.md)
  - [GetAggInitVal](../G/GetAggInitVal.md)
  - [stringTypeDatum](../s/stringTypeDatum.md)
  - [slot_store_data](../s/slot_store_data.md)
  - [exec_bind_message](../e/exec_bind_message.md)
  - [get_typdefault](../g/get_typdefault.md)

## Notes and Other Information
- This function is slow and leaks memory, so it should only be used in seldom-executed code paths
- For performance-critical code, use InputFunctionCall with pre-cached FmgrInfo instead
- The function is located in src/backend/utils/fmgr/fmgr.c at lines 1754-1762
- It's commonly used in bootstrap operations, command processing, and replication scenarios where convenience outweighs performance