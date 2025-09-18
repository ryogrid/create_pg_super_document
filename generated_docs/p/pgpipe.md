# pgpipe

## Location
[src/bin/pg_dump/parallel.c:1719-1801](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/parallel.c#L1719-L1801)

## Overview
A Windows-specific replacement for the Unix pipe(2) system call that creates socket-based pipe handles usable with select().

## Definition


## Detailed Description
 is a platform-specific function designed to address limitations of the standard Windows pipe implementation. Unlike Unix pipes, Windows pipes cannot be used with the select() function for non-blocking I/O operations. This function creates a pair of connected TCP sockets over the loopback interface to emulate pipe behavior while maintaining compatibility with select().

The function establishes a temporary listening socket on localhost, creates a client socket that connects to it, accepts the connection to create the server-side socket, and then closes the listening socket. The resulting pair of connected sockets behaves like a pipe where data written to one handle can be read from the other.

For proper functionality, reads and writes on the returned handles must go through specialized functions  and  rather than standard socket operations.

## Parameters / Member Variables
- : Array to receive the two pipe handles
  - : Read end of the pipe (server socket from accept)
  - : Write end of the pipe (client socket from connect)

## Dependencies
- Functions called/Symbols referenced:
  - socket (creates TCP sockets)
  - bind (binds listening socket to loopback)
  - listen (puts socket in listening state)
  - getsockname (retrieves assigned port number)
  - connect (connects client socket to server)
  - accept (accepts client connection)
  - closesocket (closes socket handles)
  - pg_hton16 (converts port to network byte order)
  - pg_hton32 (converts address to network byte order)
  - pg_log_error (logs error messages)
  - PGINVALID_SOCKET (invalid socket constant)
- Called from (representative examples):
  - [ParallelBackupStart](../P/ParallelBackupStart.md) (parallel backup initialization)

## Notes and Other Information
- Windows-only implementation (conditionally compiled)
- Uses loopback interface (127.0.0.1) for security and performance
- Handles are cast to int for Unix compatibility, safe on Windows where handles are 32-bit
- Error handling includes proper cleanup of partially created resources
- Temporary listening socket is immediately closed after connection establishment
- Returned handles must be used with piperead/pipewrite functions, not standard I/O
- Part of pg_dump's parallel processing infrastructure for Windows portability