# ReceiveFunctionCall

## Location
src/backend/utils/fmgr/fmgr.c: 1697 - 1743

## Overview
ReceiveFunctionCall is a convenience wrapper that calls a previously-looked-up datatype binary-input function to convert binary data to its internal Datum format.

## Definition
```c
Datum ReceiveFunctionCall(FmgrInfo *flinfo, StringInfo buf, Oid typioparam, int32 typmod)
```

## Detailed Description
This function provides the binary counterpart to InputFunctionCall, handling conversion of binary-format data rather than text format. It takes a StringInfo buffer containing binary data and converts it to the appropriate internal Datum representation using the specified receive function. The function handles NULL inputs by checking the buffer parameter and manages strictness rules similar to InputFunctionCall. It includes validation to ensure that NULL buffers produce NULL outputs and non-NULL buffers produce non-NULL outputs.

## Parameters / Member Variables
- `flinfo`: Function manager info structure containing details about the receive function to call
- `buf`: StringInfo buffer containing binary data to convert (may be NULL to indicate a NULL value)
- `typioparam`: OID parameter passed to the receive function (type-specific parameter)
- `typmod`: Type modifier value providing additional type information

## Dependencies
- Functions called/Symbols referenced:
  - LOCAL_FCINFO (macro for local function call info)
  - InitFunctionCallInfoData (initializes function call structure)
  - FunctionCallInvoke (invokes the actual function)
  - [PointerGetDatum](../P/PointerGetDatum.md) (converts pointer to Datum - implicitly used)
- Called from (representative examples):
  - [CopyReadBinaryAttribute](../C/CopyReadBinaryAttribute.md)
  - [ReadArrayBinary](ReadArrayBinary.md)
  - [domain_recv](../d/domain_recv.md)
  - [range_recv](../r/range_recv.md)
  - [record_recv](../r/record_recv.md)
  - [OidReceiveFunctionCall](../O/OidReceiveFunctionCall.md)

## Notes and Other Information
- Binary counterpart to InputFunctionCall - handles binary format instead of text format
- Used primarily in binary COPY operations and array deserialization
- Part of PostgreSQL's binary I/O system for efficient data transfer
- Similar error handling to InputFunctionCall with validation of NULL behavior
- Essential for protocol-level binary data exchange and internal serialization
- Used extensively in composite types (arrays, ranges, records, domains) for binary format processing