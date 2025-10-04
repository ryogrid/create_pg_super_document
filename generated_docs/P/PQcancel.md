# PQcancel

## Location
[src/interfaces/libpq/fe-cancel.c:464-661](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-cancel.c#L464-L661)

## Overview
Sends a cancel request to the PostgreSQL backend to terminate a currently executing query, using an old, non-encrypted but signal-safe protocol.

## Definition

```c
struct
	{
		uint32		packetlen;
		CancelRequestPacket cp;
	}			crp;
```
## Detailed Description
PQcancel implements the original PostgreSQL query cancellation mechanism. It establishes a temporary TCP connection to the PostgreSQL server and sends a cancel request packet containing the backend process ID and authentication key. The function is designed to be signal-safe, making it suitable for use in signal handlers (e.g., SIGINT). It uses only reentrant system calls and avoids malloc/free operations. The function sets up keepalive options on the socket to prevent indefinite blocking, sends the cancellation request, and waits for the server to close the connection as confirmation of receipt.

## Parameters / Member Variables
- `cancel`: Pointer to PGcancel structure containing connection details, backend PID, and authentication key
- `errbuf`: Buffer to store error messages on failure (recommended size 256 bytes)
- `errbufsize`: Size of the error buffer

## Dependencies
- Functions called/Symbols referenced:
  - socket (system call for creating socket)
  - connect (system call for establishing connection)
  - send (system call for sending data)
  - recv (system call for receiving data)
  - closesocket (socket cleanup)
  - [optional_setsockopt](../o/optional_setsockopt.md) (helper for socket options)
  - [strlcpy](../s/strlcpy.md) (safe string copying)
  - pg_hton32 (host to network byte order conversion)
  - [pqSetKeepalivesWin32](../p/pqSetKeepalivesWin32.md) (Windows keepalive configuration)
- Called from (representative examples):
  - [ShutdownWorkersHard](../S/ShutdownWorkersHard.md) (src/bin/pg_dump/parallel.c:433)
  - [sigTermHandler](../s/sigTermHandler.md) (src/bin/pg_dump/parallel.c:581)
  - [handle_sigint](../h/handle_sigint.md) (src/fe_utils/cancel.c:165)
  - [PQrequestCancel](PQrequestCancel.md) (src/interfaces/libpq/fe-cancel.c:685)
  - [test_cancel](../t/test_cancel.md) (src/test/modules/libpq_pipeline/libpq_pipeline.c:267)

## Notes and Other Information
- Signal-safe implementation suitable for use in signal handlers
- Returns true on successful dispatch, false on failure (does not guarantee query cancellation)
- Uses only reentrant functions to avoid reentrancy issues
- Configures TCP keepalive options to prevent indefinite blocking
- Implements retry logic for interrupted system calls (EINTR)
- Creates temporary socket connection specifically for the cancel request
- Error messages are built using safe string operations without sprintf
- Part of the legacy cancellation API, with newer encrypted alternatives available
- Location: src/interfaces/libpq/fe-cancel.c:464-661

## Simplified Source

```c
int PQcancel(PGcancel *cancel, char *errbuf, int errbufsize) {
    int save_errno = SOCK_ERRNO;
    pgsocket tmpsock = PGINVALID_SOCKET;
    struct {
        uint32 packetlen;
        CancelRequestPacket cp;
    } crp;

    // Validate cancel object
    if (!cancel) {
        strlcpy(errbuf, "PQcancel() -- no cancel object supplied", errbufsize);
        SOCK_ERRNO_SET(save_errno);
        return false;
    }

    // Create temporary socket
    if ((tmpsock = socket(cancel->raddr.addr.ss_family, SOCK_STREAM, 0)) == PGINVALID_SOCKET) {
        strlcpy(errbuf, "PQcancel() -- socket() failed", errbufsize);
        goto cancel_errReturn;
    }

    // Set socket keepalive options (if enabled)
    if (cancel->raddr.addr.ss_family != AF_UNIX && cancel->keepalives != 0) {
        // Configure keepalive settings for timeout prevention
        // Platform-specific socket option configuration...
    }

    // Connect to server with retry on interruption
retry_connect:
    if (connect(tmpsock, (struct sockaddr *) &cancel->raddr.addr, cancel->raddr.salen) < 0) {
        if (SOCK_ERRNO == EINTR)
            goto retry_connect;
        strlcpy(errbuf, "PQcancel() -- connect() failed", errbufsize);
        goto cancel_errReturn;
    }

    // Build and send cancel request packet
    crp.packetlen = pg_hton32((uint32) sizeof(crp));
    crp.cp.cancelRequestCode = (MsgType) pg_hton32(CANCEL_REQUEST_CODE);
    crp.cp.backendPID = pg_hton32(cancel->be_pid);
    crp.cp.cancelAuthCode = pg_hton32(cancel->be_key);

retry_send:
    if (send(tmpsock, (char *) &crp, sizeof(crp), 0) != (int) sizeof(crp)) {
        if (SOCK_ERRNO == EINTR)
            goto retry_send;
        strlcpy(errbuf, "PQcancel() -- send() failed", errbufsize);
        goto cancel_errReturn;
    }

    // Wait for server to close connection (confirmation)
retry_recv:
    if (recv(tmpsock, (char *) &crp, 1, 0) < 0) {
        if (SOCK_ERRNO == EINTR)
            goto retry_recv;
    }

    // Success cleanup
    closesocket(tmpsock);
    SOCK_ERRNO_SET(save_errno);
    return true;

cancel_errReturn:
    // Error cleanup and errno formatting
    if (tmpsock != PGINVALID_SOCKET)
        closesocket(tmpsock);
    SOCK_ERRNO_SET(save_errno);
    return false;
}
```