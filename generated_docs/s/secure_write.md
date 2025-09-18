# secure_write

## Location
src/backend/libpq/be-secure.c: 301 - 372

## Overview
secure_write provides a secure, blocking write operation to client connections that handles SSL/TLS encryption, GSS-API authentication, and raw socket communications with interrupt processing and wait event management.

## Definition


## Detailed Description
The secure_write function is the primary interface for writing data to PostgreSQL client connections in a secure manner. It serves as the counterpart to secure_read and abstracts the complexities of different security protocols (SSL/TLS, GSS-API) while providing a unified interface for secure communications. The function implements sophisticated blocking behavior management and integrates with PostgreSQL's wait event system for efficient resource utilization.

The function operates by first processing any pending client write interrupts, then determining the appropriate write mechanism based on the connection's security configuration. For SSL connections, it delegates to be_tls_write; for GSS-API encrypted connections, it uses be_gssapi_write; otherwise, it falls back to secure_raw_write for plain socket communications.

Like its read counterpart, the function implements intelligent blocking behavior: when a write operation would block due to full socket buffers (EWOULDBLOCK/EAGAIN), it uses PostgreSQL's wait event system to efficiently wait for socket writability. During these waits, it monitors for critical system events such as postmaster death and processes client write interrupts to ensure proper cleanup and responsiveness.

## Parameters / Member Variables
- : Pointer to Port structure containing connection state, security configuration, and socket information
- : Buffer containing the data to be written
- : Number of bytes to write from the buffer

## Dependencies
- Functions called/Symbols referenced:
  - [ProcessClientWriteInterrupt](../P/ProcessClientWriteInterrupt.md): Handles client write interrupt conditions
  - [be_tls_write](../b/be_tls_write.md): SSL/TLS-specific write operation (when SSL is enabled)
  - be_gssapi_write: GSS-API encrypted write operation (when GSS is enabled)
  - [secure_raw_write](secure_raw_write.md): Raw socket write for unencrypted connections
  - [ModifyWaitEvent](../M/ModifyWaitEvent.md): Updates wait event configuration for socket monitoring
  - WaitEventSetWait: Blocks until socket becomes ready or other events occur
  - [ResetLatch](../R/ResetLatch.md): Clears the process latch after interrupt processing
- Called from (representative examples):
  - internal_flush_buffer: Main packet transmission buffer management
  - [ProcessStartupPacket](../P/ProcessStartupPacket.md): Initial connection handshake data transmission

## Notes and Other Information
- The function implements a retry mechanism with goto retry label to handle interrupts and socket state changes
- Postmaster death detection ensures backend processes terminate cleanly when the postmaster exits unexpectedly  
- Wait events are used for WAIT_EVENT_CLIENT_WRITE monitoring during blocking operations
- The function respects the port->noblock setting, only entering wait states when blocking mode is enabled
- Error handling preserves errno values from underlying write operations
- Integration with PostgreSQL's interrupt handling system ensures query cancellation and other signals are processed during write operations
- Unlike reads, writes typically have more predictable blocking behavior since they depend on socket buffer space rather than incoming data availability