# secure_open_server

## Location
[src/backend/libpq/be-secure.c:110-162](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/be-secure.c#L110-L162)

## Overview
Establishes a secure SSL/TLS session with a client by negotiating encryption and handling the transition from unencrypted to encrypted communication.

## Definition

```c
int
secure_open_server(Port *port)
```
## Detailed Description
The `secure_open_server` function manages the server-side SSL/TLS handshake process and the critical transition from unencrypted to encrypted communication. It handles a complex scenario where some data may have already been buffered before the SSL negotiation begins, requiring careful management of unencrypted data that needs to be processed through the SSL layer.

The function first preserves any unencrypted data that was already read from the client, then calls `be_tls_open_server` to perform the actual SSL handshake. After successful negotiation, it verifies that no unencrypted data remains (which would indicate a protocol violation), cleans up temporary buffers, and logs connection details including the client's Distinguished Name (DN) and Common Name (CN) from the SSL certificate.

## Parameters / Member Variables
- `port`: Pointer to a Port structure representing the client connection. Contains socket information, buffering state, and SSL-related fields including peer certificate details.

## Dependencies
- Functions called/Symbols referenced:
  - [pq_buffer_remaining_data](../p/pq_buffer_remaining_data.md) (checks for buffered unencrypted data)
  - [pq_startmsgread](../p/pq_startmsgread.md)/pq_endmsgread (message reading protocol functions)
  - [pq_getbytes](../p/pq_getbytes.md) (reads buffered data)
  - [be_tls_open_server](../b/be_tls_open_server.md) (performs actual SSL handshake)
  - [palloc](../p/palloc.md)/pfree (PostgreSQL memory management)
  - ereport (PostgreSQL logging system)
  - STATUS_ERROR (error return constant)
- Called from (representative examples):
  - [ProcessSSLStartup](../P/ProcessSSLStartup.md) (during SSL connection establishment)
  - [ProcessStartupPacket](../P/ProcessStartupPacket.md) (as part of connection startup sequence)
  - FeBeWaitSetNEvents (referenced in libpq.h)

## Dependencies
- Functions called/Symbols referenced:
  - [pq_buffer_remaining_data](../p/pq_buffer_remaining_data.md)
  - [pq_startmsgread](../p/pq_startmsgread.md)
  - [pq_getbytes](../p/pq_getbytes.md)
  - [pq_endmsgread](../p/pq_endmsgread.md)
  - [be_tls_open_server](../b/be_tls_open_server.md)
  - [palloc](../p/palloc.md)
  - [pfree](../p/pfree.md)
  - ereport
  - STATUS_ERROR
  - DEBUG2
- Called from (representative examples):
  - [ProcessSSLStartup](../P/ProcessSSLStartup.md)
  - [ProcessStartupPacket](../P/ProcessStartupPacket.md)
  - FeBeWaitSetNEvents

## Notes and Other Information
- Returns 0 on success, STATUS_ERROR on failure
- Handles the complex transition from unencrypted to encrypted communication
- Logs SSL connection details at DEBUG2 level including client certificate information
- When SSL is not compiled in, the function becomes a no-op that always returns 0
- Critical for maintaining protocol integrity during SSL handshake
- The function includes assertions and error checking to detect protocol violations

## Simplified Source

```c
// Simplified version of secure_open_server
int secure_open_server(Port *port) {
#ifdef USE_SSL
    int result = 0;
    ssize_t buffered_data_len;

    // Step 1: Handle any unencrypted data that was already buffered
    buffered_data_len = pq_buffer_remaining_data();
    if (buffered_data_len > 0) {
        // Save the buffered data to process it through SSL later
        char *saved_buffer = palloc(buffered_data_len);

        pq_startmsgread();
        if (pq_getbytes(saved_buffer, buffered_data_len) == EOF)
            return STATUS_ERROR;
        pq_endmsgread();

        // Store the saved data in the port structure
        port->raw_buf = saved_buffer;
        port->raw_buf_remaining = buffered_data_len;
        port->raw_buf_consumed = 0;
    }

    // Step 2: Perform the actual SSL handshake
    result = be_tls_open_server(port);

    // Step 3: Verify protocol integrity after SSL negotiation
    if (port->raw_buf_remaining > 0) {
        // This indicates a protocol violation - client sent encrypted data too early
        elog(LOG, "buffered unencrypted data remains after negotiating SSL connection");
        return STATUS_ERROR;
    }

    // Step 4: Clean up temporary buffer
    if (port->raw_buf != NULL) {
        pfree(port->raw_buf);
        port->raw_buf = NULL;
    }

    // Step 5: Log successful SSL connection details
    ereport(DEBUG2, (errmsg_internal("SSL connection from DN:\"%s\" CN:\"%s\"",
                                     port->peer_dn ? port->peer_dn : "(anonymous)",
                                     port->peer_cn ? port->peer_cn : "(anonymous)")));

    return result;
#else
    // When SSL is not compiled in, this is a no-op
    return 0;
#endif
}
```

Key simplifications made:
- Removed detailed error handling comments for clarity
- Consolidated variable declarations at the top
- Added step-by-step comments explaining the main logic flow
- Simplified variable names (r → result, len → buffered_data_len)
- Removed assertion checks to focus on main logic
- Made the SSL vs non-SSL compilation difference more explicit
- Focused on the core SSL negotiation workflow