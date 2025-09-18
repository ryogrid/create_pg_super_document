# parse_fcall_arguments

## Location
src/backend/tcop/fastpath.c: 330 - 459

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
  - resetStringInfo
  - appendBinaryStringInfo
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