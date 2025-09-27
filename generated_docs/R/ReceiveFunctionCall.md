# ReceiveFunctionCall

## Location
[src/backend/utils/fmgr/fmgr.c:1697-1743](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/fmgr.c#L1697-L1743)

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

## Simplified Source

```c
// Simplified version of ReceiveFunctionCall
Datum ReceiveFunctionCall(FmgrInfo *flinfo, StringInfo buf, Oid typioparam, int32 typmod) {
    LOCAL_FCINFO(fcinfo, 3);
    Datum result;

    // Handle NULL input for strict functions
    if (buf == NULL && flinfo->fn_strict) {
        return (Datum) 0;  // Return NULL without calling function
    }

    // Set up function call with 3 arguments
    InitFunctionCallInfoData(*fcinfo, flinfo, 3, InvalidOid, NULL, NULL);

    // Prepare arguments: binary buffer, type IO param, type modifier
    fcinfo->args[0].value = PointerGetDatum(buf);
    fcinfo->args[0].isnull = false;
    fcinfo->args[1].value = ObjectIdGetDatum(typioparam);
    fcinfo->args[1].isnull = false;
    fcinfo->args[2].value = Int32GetDatum(typmod);
    fcinfo->args[2].isnull = false;

    // Call the binary receive function
    result = FunctionCallInvoke(fcinfo);

    // Validate NULL handling consistency
    if (buf == NULL) {
        // NULL buffer should produce NULL result
        if (!fcinfo->isnull) {
            elog(ERROR, "receive function %u returned non-NULL", flinfo->fn_oid);
        }
    } else {
        // Non-NULL buffer should produce non-NULL result
        if (fcinfo->isnull) {
            elog(ERROR, "receive function %u returned NULL", flinfo->fn_oid);
        }
    }

    return result;
}
```

Key simplifications made:
- Added clear comments explaining binary data handling
- Highlighted the difference from InputFunctionCall (binary vs text)
- Simplified argument setup with explanatory comments
- Preserved NULL validation logic with clear explanations
- Focused on the main execution path for binary data conversion