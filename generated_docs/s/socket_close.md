# socket_close

## Location
[src/backend/libpq/pqcomm.c:348-416](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/pqcomm.c#L348-L416)

## Overview
Performs cleanup of libpq communication resources at backend process exit, shutting down security layers and invalidating the socket while preserving connection state for synchronous client closure.

## Definition

```c
struct addrinfo *addrs = NULL,
			   *addr;
```
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
  - [secure_close](secure_close.md)
  - PGINVALID_SOCKET
  - gss_delete_sec_context (when GSSAPI enabled)
  - gss_release_cred (when GSSAPI enabled)
- Called from (representative examples):
  - [pq_init](../p/pq_init.md) (registered as exit callback)
  - Process exit handling

## Notes and Other Information
- This is a static function, only accessible within pqcomm.c
- Designed to be signal-safe and callable at any time during process execution
- Only active callback registered during BackendInitialize()
- Does nothing in standalone backend mode (when MyProcPort is NULL)
- Supports both GSSAPI and SSL/TLS cleanup when those features are compiled in
- The decision to not explicitly close the socket allows for better client-side connection handling

## Simplified Source

```c
// Simplified version of socket_close
static void socket_close(int code, Datum arg) {
    // Do nothing if we're in standalone backend mode
    if (MyProcPort == NULL) {
        return;
    }

    // Step 1: Clean up GSSAPI security context if enabled
    #ifdef ENABLE_GSS
    if (MyProcPort->gss) {
        // Delete security context if it exists
        if (MyProcPort->gss->ctx != GSS_C_NO_CONTEXT) {
            gss_delete_sec_context(&min_s, &MyProcPort->gss->ctx, NULL);
        }

        // Release credentials if they exist
        if (MyProcPort->gss->cred != GSS_C_NO_CREDENTIAL) {
            gss_release_cred(&min_s, &MyProcPort->gss->cred);
        }
    }
    #endif

    // Step 2: Clean shutdown of SSL/TLS layer
    secure_close(MyProcPort);

    // Step 3: Invalidate socket to prevent further I/O
    // Note: We don't explicitly close() the socket - this allows
    // clients to detect clean connection closure
    MyProcPort->sock = PGINVALID_SOCKET;
}
```

Key simplifications made:
- Removed detailed comments for conciseness while preserving essential information
- Consolidated GSSAPI cleanup logic with clearer structure
- Added step-by-step comments to show the logical flow
- Preserved the important design decision about not closing the socket
- Maintained the conditional compilation directives for GSSAPI
- Simplified variable declarations by showing only the essential parts