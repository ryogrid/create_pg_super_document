# secure_raw_read

## Location
[src/backend/libpq/be-secure.c:264-300](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/be-secure.c#L264-L300)

## Overview
secure_raw_read performs low-level socket read operations for unencrypted connections, handling buffered data management and non-blocking socket I/O with platform-specific optimizations.

## Definition

```c
ssize_t
secure_raw_read(Port *port, void *ptr, size_t len)
```
## Detailed Description
The secure_raw_read function provides the lowest-level read interface for PostgreSQL client connections when no encryption is in use. It serves as the foundation for higher-level secure read operations and handles two primary responsibilities: managing previously buffered 'unread' data and performing actual socket read operations.

The function first checks if there is any data in the port's raw buffer (raw_buf_remaining > 0). This buffered data represents bytes that were previously read from the socket but not yet consumed by the application layer. If buffered data exists, it copies the requested amount (up to the available buffer size) to the caller's buffer and updates the buffer tracking variables.

When no buffered data is available, the function performs a direct recv() call on the socket. On Windows platforms, it temporarily sets the pgwin32_noblock flag to ensure proper non-blocking behavior, demonstrating platform-specific handling requirements for socket operations.

## Parameters / Member Variables
- : Pointer to Port structure containing socket descriptor and raw buffer management fields
- : Destination buffer for the read data
- : Maximum number of bytes to read

## Dependencies
- Functions called/Symbols referenced:
  - recv: Standard POSIX socket receive function for reading data from network socket
  - memcpy: Memory copy operation for transferring buffered data to caller's buffer
- Called from (representative examples):
  - [secure_read](secure_read.md): Higher-level secure read function for non-encrypted connections
  - [be_gssapi_read](../b/be_gssapi_read.md): GSS-API read implementation uses this for underlying socket operations
  - [my_sock_read](../m/my_sock_read.md): SSL/TLS socket read operations in OpenSSL backend

## Notes and Other Information
- The function manages the Port structure's raw_buf, raw_buf_consumed, and raw_buf_remaining fields for buffer tracking
- On Windows, pgwin32_noblock flag manipulation ensures consistent non-blocking socket behavior across platforms
- Returns the actual number of bytes read, which may be less than requested due to socket buffer availability
- Does not perform any error handling or retry logic - that responsibility lies with calling functions like secure_read
- The raw buffer mechanism allows for efficient handling of partially-consumed protocol messages
- This function is the ultimate fallback for all PostgreSQL socket read operations when no encryption layer is active