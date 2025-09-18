# ProcessSSLStartup

## Location
[src/backend/tcop/backend_startup.c:362-452](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/backend_startup.c#L362-L452)

## Overview
ProcessSSLStartup checks for and handles direct SSL connection requests before the standard startup packet is processed.

## Definition


## Detailed Description
ProcessSSLStartup examines the first byte of incoming client data to determine if the client is attempting a direct SSL connection. It distinguishes SSL handshake messages (starting with 0x16) from regular PostgreSQL startup packets. When SSL is requested and supported, it establishes the SSL connection and validates that ALPN protocol negotiation was used. The function is careful not to consume data from the stream unless it's confirmed to be an SSL handshake, allowing proper fallback to standard startup packet processing.

## Parameters / Member Variables
- : Port structure representing the client connection

## Dependencies
- Functions called/Symbols referenced:
  - [pq_startmsgread](../p/pq_startmsgread.md)
  - [pq_peekbyte](../p/pq_peekbyte.md)  
  - [pq_endmsgread](../p/pq_endmsgread.md)
  - [secure_open_server](../s/secure_open_server.md) (SSL builds)
- Called from (representative examples):
  - [BackendInitialize](../B/BackendInitialize.md)

## Notes and Other Information
- Returns STATUS_OK if not an SSL request or if SSL connection established successfully
- Returns STATUS_ERROR if connection should be rejected (EOF, SSL not supported, SSL setup failed)
- Requires ALPN protocol negotiation extension for direct SSL connections
- SSL not supported on Unix domain sockets (AF_UNIX)
- First byte 0x16 indicates standard SSL handshake message
- Does not support SSL if built without USE_SSL or if SSL library not loaded
- Uses Trace_connection_negotiation for debugging SSL connection attempts
- Located in src/backend/tcop/backend_startup.c:362-452