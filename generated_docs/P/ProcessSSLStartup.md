# ProcessSSLStartup

## Location
[src/backend/tcop/backend_startup.c:362-452](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/backend_startup.c#L362-L452)

## Overview
ProcessSSLStartup checks for and handles direct SSL connection requests before the standard startup packet is processed.

## Definition

```c
static int
ProcessSSLStartup(Port *port)
```
## Detailed Description
ProcessSSLStartup examines the first byte of incoming client data to determine if the client is attempting a direct SSL connection. It distinguishes SSL handshake messages (starting with 0x16) from regular PostgreSQL startup packets. When SSL is requested and supported, it establishes the SSL connection and validates that ALPN protocol negotiation was used. The function is careful not to consume data from the stream unless it's confirmed to be an SSL handshake, allowing proper fallback to standard startup packet processing.

## Parameters / Member Variables
- `*port`: Port structure representing the client connection
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

## Simplified Source

```c
// Simplified version of ProcessSSLStartup
static int ProcessSSLStartup(Port *port) {
    int firstbyte;

    Assert(!port->ssl_in_use);

    // Core logic step 1: Peek at the first byte without consuming it
    pq_startmsgread();
    firstbyte = pq_peekbyte();
    pq_endmsgread();

    // Core logic step 2: Handle connection errors
    if (firstbyte == EOF) {
        return STATUS_ERROR;  // No data received
    }

    // Core logic step 3: Check if this is an SSL handshake
    if (firstbyte != 0x16) {
        return STATUS_OK;  // Not SSL, continue with normal startup
    }

    // Core logic step 4: Process SSL connection request
#ifdef USE_SSL
    // Validate SSL is available and supported
    if (!LoadedSSL || port->laddr.addr.ss_family == AF_UNIX) {
        goto reject;  // SSL not supported
    }

    // Establish SSL connection
    if (secure_open_server(port) == -1) {
        goto reject;  // SSL setup failed
    }

    Assert(port->ssl_in_use);

    // Validate ALPN protocol negotiation was used
    if (!port->alpn_used) {
        ereport(COMMERROR,
                (errcode(ERRCODE_PROTOCOL_VIOLATION),
                 errmsg("received direct SSL connection request without ALPN protocol negotiation extension")));
        goto reject;
    }

    // Core logic step 5: SSL connection successful
    if (Trace_connection_negotiation) {
        ereport(LOG, (errmsg("direct SSL connection accepted")));
    }
    return STATUS_OK;

#else
    goto reject;  // SSL not compiled in
#endif

reject:
    if (Trace_connection_negotiation) {
        ereport(LOG, (errmsg("direct SSL connection rejected")));
    }
    return STATUS_ERROR;
}
```

Key simplifications made:
- Removed detailed comments about startup packet length validation
- Consolidated error handling paths using goto reject pattern
- Focused on the main execution flow: peek byte → check if SSL → establish SSL → validate ALPN
- Abstracted SSL-specific details while preserving critical validation steps
- Maintained the essential algorithm: non-destructive peek, SSL detection, secure connection establishment