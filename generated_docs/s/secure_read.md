# secure_read

## Location
src/backend/libpq/be-secure.c: 175 - 263

## Overview
secure_read provides a secure, blocking read operation from a client connection that handles SSL/TLS encryption, GSS-API authentication, and raw socket communications with interrupt processing and wait event management.

## Definition


## Detailed Description
The secure_read function is the primary interface for reading data from PostgreSQL client connections in a secure manner. It abstracts away the complexities of different security protocols (SSL/TLS, GSS-API) and provides a unified interface for secure communications. The function implements a sophisticated blocking mechanism that handles client read interrupts and integrates with PostgreSQL's wait event system.

The function operates by first checking for pending client read interrupts, then determining the appropriate read mechanism based on the connection's security configuration. For SSL connections, it delegates to be_tls_read; for GSS-API encrypted connections, it uses be_gssapi_read; otherwise, it falls back to secure_raw_read for plain socket communications.

A key feature is its blocking behavior management: when a read would block (EWOULDBLOCK/EAGAIN), it uses PostgreSQL's wait event system to efficiently wait for socket readiness rather than busy-waiting. During waits, it monitors for critical events like postmaster death and client interrupts, ensuring proper cleanup and responsiveness to administrative signals.

## Parameters / Member Variables
- : Pointer to Port structure containing connection state, security configuration, and socket information
- : Buffer to store the read data
- : Maximum number of bytes to read

## Dependencies
- Functions called/Symbols referenced:
  - [ProcessClientReadInterrupt](../P/ProcessClientReadInterrupt.md): Handles client read interrupt conditions
  - [be_tls_read](../b/be_tls_read.md): SSL/TLS-specific read operation (when SSL is enabled)
  - [be_gssapi_read](../b/be_gssapi_read.md): GSS-API encrypted read operation (when GSS is enabled)  
  - [secure_raw_read](secure_raw_read.md): Raw socket read for unencrypted connections
  - [ModifyWaitEvent](../M/ModifyWaitEvent.md): Updates wait event configuration for socket monitoring
  - WaitEventSetWait: Blocks until socket becomes ready or other events occur
  - [ResetLatch](../R/ResetLatch.md): Clears the process latch after interrupt processing
- Called from (representative examples):
  - [pq_recvbuf](../p/pq_recvbuf.md): Main packet receive buffer management
  - [pq_getbyte_if_available](../p/pq_getbyte_if_available.md): Non-blocking single byte retrieval

## Notes and Other Information
- The function implements a retry mechanism with goto retry label to handle interrupts and socket state changes
- Postmaster death detection ensures backend processes terminate cleanly when the postmaster exits unexpectedly
- Wait events are used for WAIT_EVENT_CLIENT_READ monitoring during blocking operations
- The function respects the port->noblock setting, only entering wait states when blocking mode is enabled
- Error handling preserves errno values from underlying read operations
- Integration with PostgreSQL's interrupt handling system ensures query cancellation and other signals are processed promptly