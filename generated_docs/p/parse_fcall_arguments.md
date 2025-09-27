# parse_fcall_arguments

## Location
[src/backend/tcop/fastpath.c:330-459](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/fastpath.c#L330-L459)

## Overview
Parses function arguments from a PostgreSQL 3.0 protocol message, converting them to appropriate Datum values and loading them into the function call info structure.

## Definition
static int16 parse_fcall_arguments(StringInfo msgBuf, struct fp_info *fip, FunctionCallInfo fcinfo)

## Detailed Description
parse_fcall_arguments is responsible for extracting and processing function arguments from the client message buffer in the fast-path protocol. It handles both text (format 0) and binary (format 1) argument formats, performing necessary type conversions using the appropriate input functions. The function validates argument counts, processes format codes, and handles NULL values properly. For text format arguments, it performs client-to-server encoding conversion before calling the type's input function. For binary format arguments, it uses the type's receive function directly. The function also validates that binary data is fully consumed and returns the desired result format code. It supports flexible format specification where formats can be specified per-argument or globally.

## Parameters / Member Variables
- : StringInfo containing the message buffer with argument data from the client
- : Pointer to fp_info structure containing function metadata including argument types
- : FunctionCallInfo structure to be populated with parsed argument values

## Dependencies
- Functions called/Symbols referenced:
  - [fp_info](../f/fp_info.md)
  - [FunctionCallInfo](../F/FunctionCallInfo.md)
  - [pq_getmsgint](pq_getmsgint.md)
  - FUNC_MAX_ARGS
  - [pq_getmsgbytes](pq_getmsgbytes.md)
  - [resetStringInfo](../r/resetStringInfo.md)
  - [appendBinaryStringInfo](../a/appendBinaryStringInfo.md)
  - [getTypeInputInfo](../g/getTypeInputInfo.md)
  - [pg_client_to_server](pg_client_to_server.md)
  - [OidInputFunctionCall](../O/OidInputFunctionCall.md)
  - [getTypeBinaryInputInfo](../g/getTypeBinaryInputInfo.md)
  - [OidReceiveFunctionCall](../O/OidReceiveFunctionCall.md)
- Called from (representative examples):
  - [HandleFunctionRequest](../H/HandleFunctionRequest.md)

## Notes and Other Information
- This is a static function used internally within the fast-path protocol implementation
- Supports flexible argument format specification: all arguments can use the same format, or each argument can have its own format
- Performs comprehensive validation including argument count matching and format code validation
- Handles NULL arguments properly by setting the isnull flag and skipping conversion
- For text format, performs encoding conversion from client to server encoding before type input function calls
- For binary format, validates that the entire buffer is consumed by the receive function
- Returns the result format code (0 for text, 1 for binary) requested by the client
- Memory management includes proper cleanup of encoding conversion results
- Uses StringInfo buffer for efficient handling of variable-length argument data

## Simplified Source

```c
// Simplified version of parse_fcall_arguments
static int16 parse_fcall_arguments(StringInfo msgBuf, struct fp_info *fip, FunctionCallInfo fcinfo) {
    int numAFormats, nargs, i;
    int16 *aformats = NULL;
    StringInfoData abuf;

    // Read argument format codes from message
    numAFormats = pq_getmsgint(msgBuf, 2);
    if (numAFormats > 0) {
        aformats = palloc(numAFormats * sizeof(int16));
        for (i = 0; i < numAFormats; i++) {
            aformats[i] = pq_getmsgint(msgBuf, 2);
        }
    }

    // Read number of arguments and validate
    nargs = pq_getmsgint(msgBuf, 2);
    if (fip->flinfo.fn_nargs != nargs || nargs > FUNC_MAX_ARGS) {
        ereport(ERROR, "function argument count mismatch");
    }

    // Validate format count matches argument count
    if (numAFormats > 1 && numAFormats != nargs) {
        ereport(ERROR, "format count mismatch");
    }

    fcinfo->nargs = nargs;
    initStringInfo(&abuf);

    // Process each argument
    for (i = 0; i < nargs; i++) {
        int argsize = pq_getmsgint(msgBuf, 4);
        int16 aformat;

        // Handle NULL arguments
        if (argsize == -1) {
            fcinfo->args[i].isnull = true;
            continue;
        }

        // Validate argument size and read argument data
        fcinfo->args[i].isnull = false;
        resetStringInfo(&abuf);
        appendBinaryStringInfo(&abuf, pq_getmsgbytes(msgBuf, argsize), argsize);

        // Determine format for this argument
        if (numAFormats > 1) {
            aformat = aformats[i];
        } else if (numAFormats > 0) {
            aformat = aformats[0];
        } else {
            aformat = 0;  // default = text
        }

        // Convert argument based on format
        if (aformat == 0) {
            // Text format: use input function with encoding conversion
            Oid typinput, typioparam;
            getTypeInputInfo(fip->argtypes[i], &typinput, &typioparam);
            char *pstring = pg_client_to_server(abuf.data, argsize);
            fcinfo->args[i].value = OidInputFunctionCall(typinput, pstring, typioparam, -1);
        } else if (aformat == 1) {
            // Binary format: use receive function directly
            Oid typreceive, typioparam;
            getTypeBinaryInputInfo(fip->argtypes[i], &typreceive, &typioparam);
            fcinfo->args[i].value = OidReceiveFunctionCall(typreceive, &abuf, typioparam, -1);
        } else {
            ereport(ERROR, "unsupported format code");
        }
    }

    // Return result format code
    return (int16) pq_getmsgint(msgBuf, 2);
}
```

Key simplifications made:
- Consolidated error handling into simplified messages
- Removed detailed error codes and messages for brevity
- Simplified the argument format determination logic
- Abstracted encoding conversion cleanup details
- Focused on the main parsing and conversion flow
- Removed detailed binary format validation for clarity