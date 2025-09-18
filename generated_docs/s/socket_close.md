# socket_close

## Location
src/backend/libpq/pqcomm.c: 348 - 416

## Overview
Performs cleanup of libpq communication resources at backend process exit, shutting down security layers and invalidating the socket while preserving connection state for synchronous client closure.

## Definition


## Detailed Description
The  function is a process exit callback that handles the orderly shutdown of the libpq communication layer when a backend process terminates. It is designed to be safely callable at any instant during process execution, making it suitable for use as an exit callback even during initialization phases.

Key cleanup operations performed:
- Shuts down GSSAPI security context and credentials if GSS authentication was used
- Cleanly closes the SSL/TLS layer through 
- Invalidates the socket descriptor to prevent further I/O operations
- Notably does NOT explicitly close the socket file descriptor

The function deliberately leaves the socket open until the process actually dies, allowing clients to perform "synchronous close" operations - clients can wait for the transport layer to report connection closure and be confident the backend has fully exited.

## Parameters / Member Variables
- : Exit code (standard pg_on_exit_callback parameter, unused in this function)
- : Callback argument (standard pg_on_exit_callback parameter, unused in this function)

## Dependencies
- Functions called/Symbols referenced:
  - secure_close
  - PGINVALID_SOCKET
  - gss_delete_sec_context (when GSSAPI enabled)
  - gss_release_cred (when GSSAPI enabled)
- Called from (representative examples):
  - pq_init (registered as exit callback)
  - Process exit handling

## Notes and Other Information
- This is a static function, only accessible within pqcomm.c
- Designed to be signal-safe and callable at any time during process execution
- Only active callback registered during BackendInitialize()
- Does nothing in standalone backend mode (when MyProcPort is NULL)
- Supports both GSSAPI and SSL/TLS cleanup when those features are compiled in
- The decision to not explicitly close the socket allows for better client-side connection handling