# pg_server_to_client

## Location
src/backend/utils/mb/mbutils.c: 738 - 748

## Overview
A utility function that converts text from the server's character encoding to the client's character encoding for protocol communication.

## Definition
```c
char *pg_server_to_client(const char *s, int len)
```

## Detailed Description
The `pg_server_to_client` function is a convenience wrapper that converts strings from the server's database encoding to the current client's encoding. It leverages the more general `pg_server_to_any` function by automatically providing the client encoding as the target encoding. This function is crucial for PostgreSQL's client-server communication, ensuring that query results and messages sent to clients are properly encoded in the client's expected character encoding.

The function is part of PostgreSQL's character encoding conversion system that enables the server to communicate effectively with clients using different character encodings while maintaining internal consistency.

## Parameters / Member Variables
- `s`: Pointer to the input string in server encoding
- `len`: Length of the input string in bytes

## Dependencies
- Functions called/Symbols referenced:
  - pg_server_to_any (performs the actual encoding conversion)
  - ClientEncoding (global variable containing current client encoding info)
- Called from (representative examples):
  - pq_sendcountedtext (sends text with length prefix)
  - pq_sendtext (sends text data in protocol messages)
  - pq_sendstring (sends null-terminated strings)
  - pq_puttextmessage (sends text messages to client)
  - pq_writestring (writes strings to output buffer)

## Notes and Other Information
- Returns a newly allocated string in client encoding that must be freed by caller
- Uses the global `ClientEncoding` variable to determine target encoding
- Essential for protocol-level communication with clients
- Counterpart to `pg_client_to_server` for bidirectional encoding conversion
- Heavily used in the message formatting and protocol output system
- Located in src/backend/utils/mb/mbutils.c:738-748