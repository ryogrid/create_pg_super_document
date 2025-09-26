# SendFunctionResult

## Location
[src/backend/tcop/fastpath.c:68-119](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/fastpath.c#L68-L119)

## Overview
Sends the result of a function call back to the client in the PostgreSQL fast-path protocol, handling both text and binary output formats as well as NULL values.

## Definition

```c
static void
SendFunctionResult(Datum retval, bool isnull, Oid rettype, int16 format)
```
## Detailed Description
SendFunctionResult is responsible for formatting and transmitting function call results to the client through the PostgreSQL message protocol. It handles three distinct cases: NULL values, text format output (format=0), and binary format output (format=1). The function uses the appropriate type output functions to convert the Datum value into the requested format and sends it as a FunctionCallResponse message. For text format, it uses the type's output function, while for binary format it uses the type's send function. The function ensures proper message framing and handles memory management for the converted output strings.

## Parameters / Member Variables
- : The Datum value returned by the function call
- : Boolean flag indicating whether the return value is NULL
- : OID of the return type, used to determine appropriate output functions
- : Output format code (0 for text, 1 for binary)

## Dependencies
- Functions called/Symbols referenced:
  - [pq_beginmessage](../p/pq_beginmessage.md)
  - PqMsg_FunctionCallResponse
  - [pq_sendint32](../p/pq_sendint32.md)
  - [getTypeOutputInfo](../g/getTypeOutputInfo.md)
  - [OidOutputFunctionCall](../O/OidOutputFunctionCall.md)
  - [pq_sendcountedtext](../p/pq_sendcountedtext.md)
  - [getTypeBinaryOutputInfo](../g/getTypeBinaryOutputInfo.md)
  - [OidSendFunctionCall](../O/OidSendFunctionCall.md)
  - [pq_sendbytes](../p/pq_sendbytes.md)
  - VARSIZE
  - VARDATA
  - [pq_endmessage](../p/pq_endmessage.md)
- Called from (representative examples):
  - [HandleFunctionRequest](../H/HandleFunctionRequest.md)

## Notes and Other Information
- This is a static function used internally within the fast-path protocol implementation
- The function validates the format parameter and reports an error for unsupported format codes
- Memory allocated for output strings/bytes is properly freed using pfree()
- For NULL values, sends -1 as the length indicator
- Binary format handling involves extracting the actual data from the bytea structure using VARDATA and VARSIZE macros