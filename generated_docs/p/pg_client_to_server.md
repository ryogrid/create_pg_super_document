# pg_client_to_server

## Location
src/backend/utils/mb/mbutils.c: 660 - 675

## Overview
A utility function that converts text from the client's character encoding to the server's character encoding.

## Definition
```c
char *pg_client_to_server(const char *s, int len)
```

## Detailed Description
The `pg_client_to_server` function is a convenience wrapper that converts a string from the current client encoding to the server encoding. It leverages the more general `pg_any_to_server` function by automatically providing the client encoding as the source encoding. This function is essential for PostgreSQL's communication protocol, ensuring that text data sent by clients is properly converted to the server's internal encoding for processing and storage.

The function is part of PostgreSQL's character encoding conversion system that enables the server to work with clients using different character encodings while maintaining consistency in internal data representation.

## Parameters / Member Variables
- `s`: Pointer to the input string in client encoding
- `len`: Length of the input string in bytes

## Dependencies
- Functions called/Symbols referenced:
  - pg_any_to_server (performs the actual encoding conversion)
  - ClientEncoding (global variable containing current client encoding info)
- Called from (representative examples):
  - pq_getmsgtext (message parsing in protocol communication)
  - pq_getmsgstring (string extraction from protocol messages)
  - parse_fcall_arguments (function call argument processing)
  - exec_bind_message (prepared statement parameter binding)

## Notes and Other Information
- Returns a newly allocated string in server encoding that must be freed by caller
- Uses the global `ClientEncoding` variable to determine source encoding
- Part of PostgreSQL's protocol-level character encoding conversion system
- Critical for ensuring proper text handling in client-server communication
- Located in src/backend/utils/mb/mbutils.c:660-675