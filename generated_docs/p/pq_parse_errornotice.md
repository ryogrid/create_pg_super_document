# pq_parse_errornotice

## Location
[src/backend/libpq/pqmq.c:216-330](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/pqmq.c#L216-L330)

## Overview
Parses an ErrorResponse or NoticeResponse protocol message and populates an ErrorData structure with the extracted error information.

## Definition
```c
void pq_parse_errornotice(StringInfo msg, ErrorData *edata)
```

## Detailed Description
The pq_parse_errornotice function is responsible for parsing PostgreSQL protocol ErrorResponse and NoticeResponse messages received from worker processes in parallel operations. These messages follow the PostgreSQL frontend/backend protocol format and contain structured error information with various diagnostic fields.

The function iterates through the message payload, extracting field codes and their corresponding values. Each field is identified by a single character code (defined by PG_DIAG_* constants) followed by a null-terminated string value. The function populates the provided ErrorData structure with the parsed information, which can then be used to reconstruct the error or notice in the receiving process.

Key parsing logic includes:
- Severity level mapping from string values to PostgreSQL internal error levels
- SQLSTATE code validation and conversion
- String field extraction with memory duplication
- Numeric field parsing for positions and line numbers
- Comprehensive error handling for invalid field codes and values

The function handles all standard PostgreSQL diagnostic fields including message text, detail, hint, context, schema/table/column names, source file information, and more.

## Parameters / Member Variables
- `msg`: StringInfo containing the ErrorResponse or NoticeResponse message payload to parse
- `edata`: Pointer to ErrorData structure that will be populated with parsed error information

## Dependencies
- Functions called/Symbols referenced:
  - MemSet: Initializes the ErrorData structure to zero
  - [pq_getmsgbyte](pq_getmsgbyte.md): Extracts a single byte (field code) from the message
  - [pq_getmsgend](pq_getmsgend.md): Verifies the message has been fully consumed
  - [pq_getmsgrawstring](pq_getmsgrawstring.md): Extracts a null-terminated string from the message
  - [pstrdup](pstrdup.md): Duplicates strings in the current memory context
  - [pg_strtoint32](pg_strtoint32.md): Converts string to 32-bit integer
  - MAKE_SQLSTATE: Constructs SQLSTATE code from individual characters
  - elog: Reports errors for invalid field codes or values
- Called from (representative examples):
  - [HandleParallelMessage](../H/HandleParallelMessage.md): Processes messages from parallel workers
  - [HandleParallelApplyMessage](../H/HandleParallelApplyMessage.md): Processes messages from logical parallel apply workers

## Notes and Other Information
- Located in src/backend/libpq/pqmq.c at lines 216-330
- This is a public function (non-static) exported via pqmq.h
- The function supports all PostgreSQL diagnostic field types defined in the protocol
- Memory for string fields is allocated using pstrdup in the current memory context
- Severity parsing includes special handling for DEBUG levels and LOG vs LOG_SERVER_ONLY
- The function validates SQLSTATE codes to ensure they are exactly 5 characters
- Unrecognized field codes result in ERROR-level log messages
- The ErrorData structure is initialized with reasonable defaults before parsing
- Used primarily in parallel query execution to forward errors from worker processes to the leader