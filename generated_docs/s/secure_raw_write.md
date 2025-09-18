# secure_raw_write

## Location
src/backend/libpq/be-secure.c: 373 - 386

## Overview
secure_raw_write performs low-level socket write operations for unencrypted connections, providing direct socket transmission with platform-specific non-blocking behavior optimization.

## Definition


## Detailed Description
The secure_raw_write function provides the lowest-level write interface for PostgreSQL client connections when no encryption is in use. It serves as the foundation for higher-level secure write operations by performing direct socket transmission using the standard POSIX send() system call.

The function is deliberately simple and focused, providing a thin wrapper around the send() system call while handling platform-specific requirements. On Windows platforms, it temporarily sets the pgwin32_noblock flag to ensure consistent non-blocking socket behavior, demonstrating the need for platform-specific optimizations in network I/O operations.

Unlike its read counterpart (secure_raw_read), this function does not handle any buffering mechanisms since write operations typically do not require the same kind of data buffering that reads do. The function directly transmits the provided data to the socket and returns the result from the underlying send() call.

## Parameters / Member Variables
- : Pointer to Port structure containing the socket descriptor for the client connection
- : Constant pointer to the buffer containing data to be transmitted (marked const to indicate data won't be modified)
- : Number of bytes to write from the buffer

## Dependencies
- Functions called/Symbols referenced:
  - send: Standard POSIX socket send function for transmitting data over network socket
- Called from (representative examples):
  - [secure_write](secure_write.md): Higher-level secure write function for non-encrypted connections
  - be_gssapi_write: GSS-API write implementation uses this for underlying socket operations
  - [my_sock_write](../m/my_sock_write.md): SSL/TLS socket write operations in OpenSSL backend

## Notes and Other Information
- The function uses const void *ptr parameter to indicate that the source data will not be modified during transmission
- On Windows, pgwin32_noblock flag manipulation ensures consistent non-blocking socket behavior across platforms
- Returns the actual number of bytes sent, which may be less than requested due to socket buffer limitations
- Does not perform any error handling, retry logic, or interrupt processing - that responsibility lies with calling functions like secure_write
- This function represents the ultimate fallback for all PostgreSQL socket write operations when no encryption layer is active
- The simplicity of this function reflects the direct nature of raw socket operations compared to the complexity of encrypted communications
- Platform-specific handling demonstrates PostgreSQL's commitment to consistent behavior across different operating systems