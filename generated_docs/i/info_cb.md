# info_cb

## Location
[src/backend/libpq/be-secure-openssl.c:1272-1322](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/be-secure-openssl.c#L1272-L1322)

## Overview
A static callback function that logs SSL connection state information and events to the PostgreSQL server log for debugging purposes.

## Definition

```c
static void
info_cb(const SSL *ssl, int type, int args)
```
## Detailed Description
The  function serves as an OpenSSL information callback that captures and logs various SSL/TLS connection events and state transitions. It is registered with OpenSSL to provide detailed debugging information about SSL handshake processes, connection states, and alert conditions. The function uses PostgreSQL's  mechanism to log messages at DEBUG4 level, making SSL connection diagnostics available through the standard PostgreSQL logging system.

The callback handles multiple SSL event types including handshake start/completion, accept/connect loop states, and SSL alert conditions. Each event type is logged with descriptive text obtained from OpenSSL's state description functions.

## Parameters / Member Variables
- `*ssl`: Pointer to the SSL connection structure providing context about the current SSL session
- `type`: Integer flag indicating the type of SSL event or state change being reported
- `args`: Additional arguments or status codes associated with the SSL event (used for exit codes and alert details)
## Dependencies
- Functions called/Symbols referenced:
  - SSL_state_string_long (OpenSSL function)
  - ereport (PostgreSQL logging function)
  - [errmsg_internal](../e/errmsg_internal.md) (PostgreSQL error message formatting)
  - DEBUG4 (PostgreSQL log level constant)
- Called from (representative examples):
  - [be_tls_open_server](../b/be_tls_open_server.md) (registered as SSL info callback)

## Notes and Other Information
- This function is only active when PostgreSQL is compiled with OpenSSL support
- Messages are logged at DEBUG4 level, requiring appropriate log_min_messages configuration to be visible
- The function handles 8 different SSL event types: handshake start/done, accept loop/exit, connect loop/exit, and read/write alerts
- Alert messages include hexadecimal codes for detailed SSL protocol diagnostics
- This callback provides essential debugging information for SSL/TLS connection troubleshooting in PostgreSQL

## Simplified Source

```c
// Simplified version of info_cb
static void
info_cb(const SSL *ssl, int type, int args)
{
    // Get human-readable SSL state description
    const char *desc = SSL_state_string_long(ssl);

    // Log different SSL events for debugging
    switch (type) {
        case SSL_CB_HANDSHAKE_START:
        case SSL_CB_HANDSHAKE_DONE:
            // Log handshake events
            ereport(DEBUG4, (errmsg_internal("SSL: handshake %s: \"%s\"",
                type == SSL_CB_HANDSHAKE_START ? "start" : "done", desc)));
            break;

        case SSL_CB_ACCEPT_LOOP:
        case SSL_CB_ACCEPT_EXIT:
            // Log server accept events (with exit code if applicable)
            ereport(DEBUG4, (errmsg_internal("SSL: accept %s: \"%s\"",
                type == SSL_CB_ACCEPT_LOOP ? "loop" : "exit", desc)));
            break;

        case SSL_CB_CONNECT_LOOP:
        case SSL_CB_CONNECT_EXIT:
            // Log client connect events (with exit code if applicable)
            ereport(DEBUG4, (errmsg_internal("SSL: connect %s: \"%s\"",
                type == SSL_CB_CONNECT_LOOP ? "loop" : "exit", desc)));
            break;

        case SSL_CB_READ_ALERT:
        case SSL_CB_WRITE_ALERT:
            // Log SSL alerts with protocol codes
            ereport(DEBUG4, (errmsg_internal("SSL: %s alert (0x%04x): \"%s\"",
                type == SSL_CB_READ_ALERT ? "read" : "write", args, desc)));
            break;
    }
}
```

Key simplifications made:
- Consolidated similar switch cases to reduce repetition
- Added inline comments explaining the purpose of each event group
- Simplified conditional logic within ereport calls
- Combined handshake, accept, connect, and alert cases into logical groups
- Preserved all essential functionality while improving readability