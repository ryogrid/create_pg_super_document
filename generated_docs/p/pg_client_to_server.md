# pg_client_to_server

## Location
[src/backend/utils/mb/mbutils.c:660-675](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/mbutils.c#L660-L675)

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
  - [pg_any_to_server](pg_any_to_server.md) (performs the actual encoding conversion)
  - ClientEncoding (global variable containing current client encoding info)
- Called from (representative examples):
  - [pq_getmsgtext](pq_getmsgtext.md) (message parsing in protocol communication)
  - [pq_getmsgstring](pq_getmsgstring.md) (string extraction from protocol messages)
  - [parse_fcall_arguments](parse_fcall_arguments.md) (function call argument processing)
  - [exec_bind_message](../e/exec_bind_message.md) (prepared statement parameter binding)

## Notes and Other Information
- Returns a newly allocated string in server encoding that must be freed by caller
- Uses the global `ClientEncoding` variable to determine source encoding
- Part of PostgreSQL's protocol-level character encoding conversion system
- Critical for ensuring proper text handling in client-server communication
- Located in src/backend/utils/mb/mbutils.c:660-675

## Simplified Source

```c
// Simplified version of pg_client_to_server
char *pg_client_to_server(const char *s, int len) {
    // Convert from client encoding to server encoding
    // Uses the global ClientEncoding to determine source encoding
    return pg_any_to_server(s, len, ClientEncoding->encoding);
}
```

Key simplifications made:
- Function is already very simple - only a single call delegation
- Added comments to clarify the purpose and mechanism
- Preserved the essential logic: delegating to pg_any_to_server with client encoding