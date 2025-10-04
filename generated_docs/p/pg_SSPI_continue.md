# pg_SSPI_continue

## Location
[src/interfaces/libpq/fe-auth.c:218-350](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-auth.c#L218-L350)

## Overview
Continues SSPI authentication with the next token in a multi-step Windows authentication handshake between the PostgreSQL client and server.

## Definition

```c
structure.
		 */
		inputbuf = malloc(payloadlen);
```
## Detailed Description
This function manages the continuation of SSPI (Security Support Provider Interface) authentication after the initial startup on Windows systems. It handles the multi-round token exchange required for Windows integrated authentication protocols like Kerberos and NTLM. The function performs these key operations:

1. **Token Processing**: Reads incoming authentication tokens from the server into SecBuffer structures
2. **Security Context Management**: Calls  to advance the authentication state machine
3. **Buffer Management**: Properly allocates, manages, and frees security buffers for token exchange
4. **Response Generation**: Sends generated authentication tokens back to the server via password packets
5. **State Tracking**: Manages the SSPI context handle and completion status

The function handles both initial calls (no existing context) and continuation calls (with server response tokens) in the multi-step authentication process.

## Parameters / Member Variables
- : PostgreSQL connection structure containing SSPI context, credentials, and connection state
- : Length of the incoming authentication token from the server (0 for initial call)

## Dependencies
- Functions called/Symbols referenced:
  -  - Memory allocation for input buffers and context handle
  -  - Reads authentication token from connection buffer  
  -  - Core SSPI function to advance authentication
  -  - Sends response token to server using GSS response packet type
  -  - Releases SSPI-allocated output buffers
  -  - Error reporting for SSPI failures
  -  - Connection error reporting
- Called from (representative examples):
  -  - Initial SSPI authentication setup
  -  - Main authentication dispatcher

## Notes and Other Information
- This is a static function internal to the libpq authentication module on Windows
- Requires SSPI/Windows authentication support to be compiled and available
- Uses SecBuffer structures for proper token exchange with Windows security APIs
- Handles context creation on first call by allocating and copying the new context handle
- Supports zero-length final tokens when negotiation completes but no data needs transmission
- Validates that SSPI returns exactly one output buffer (expected for Kerberos/NTLM)
- Sets  when authentication completes successfully
- Memory cleanup for SSPI context is handled by
- Returns STATUS_OK on success, STATUS_ERROR on failure
- Uses  packet type for compatibility with server expectations

## Simplified Source

```c
static int pg_SSPI_continue(PGconn *conn, int payloadlen) {
    SECURITY_STATUS r;
    CtxtHandle newContext;
    ULONG contextAttr;
    SecBufferDesc inbuf;
    SecBufferDesc outbuf;
    SecBuffer OutBuffers[1];
    SecBuffer InBuffers[1];
    char *inputbuf = NULL;

    // Read input token from server (if continuing authentication)
    if (conn->sspictx != NULL) {
        inputbuf = malloc(payloadlen);
        if (!inputbuf) {
            libpq_append_conn_error(conn, "out of memory allocating SSPI buffer (%d)", payloadlen);
            return STATUS_ERROR;
        }
        if (pqGetnchar(inputbuf, payloadlen, conn)) {
            free(inputbuf);
            return STATUS_ERROR;
        }

        // Setup input SecBuffer structure
        inbuf.ulVersion = SECBUFFER_VERSION;
        inbuf.cBuffers = 1;
        inbuf.pBuffers = InBuffers;
        InBuffers[0].pvBuffer = inputbuf;
        InBuffers[0].cbBuffer = payloadlen;
        InBuffers[0].BufferType = SECBUFFER_TOKEN;
    }

    // Setup output SecBuffer structure
    OutBuffers[0].pvBuffer = NULL;
    OutBuffers[0].BufferType = SECBUFFER_TOKEN;
    OutBuffers[0].cbBuffer = 0;
    outbuf.cBuffers = 1;
    outbuf.pBuffers = OutBuffers;
    outbuf.ulVersion = SECBUFFER_VERSION;

    // Perform SSPI authentication step
    r = InitializeSecurityContext(conn->sspicred, conn->sspictx, conn->sspitarget,
                                  ISC_REQ_ALLOCATE_MEMORY, 0, SECURITY_NETWORK_DREP,
                                  (conn->sspictx == NULL) ? NULL : &inbuf, 0,
                                  &newContext, &outbuf, &contextAttr, NULL);

    free(inputbuf);

    if (r != SEC_E_OK && r != SEC_I_CONTINUE_NEEDED) {
        pg_SSPI_error(conn, libpq_gettext("SSPI continuation error"), r);
        return STATUS_ERROR;
    }

    // Save context handle on first call
    if (conn->sspictx == NULL) {
        conn->sspictx = malloc(sizeof(CtxtHandle));
        if (conn->sspictx == NULL) {
            libpq_append_conn_error(conn, "out of memory");
            return STATUS_ERROR;
        }
        memcpy(conn->sspictx, &newContext, sizeof(CtxtHandle));
    }

    // Send response token to server if generated
    if (outbuf.cBuffers > 0 && outbuf.pBuffers[0].cbBuffer > 0) {
        if (pqPacketSend(conn, PqMsg_GSSResponse,
                         outbuf.pBuffers[0].pvBuffer, outbuf.pBuffers[0].cbBuffer)) {
            FreeContextBuffer(outbuf.pBuffers[0].pvBuffer);
            return STATUS_ERROR;
        }
        FreeContextBuffer(outbuf.pBuffers[0].pvBuffer);
    }

    if (r == SEC_E_OK)
        conn->client_finished_auth = true;

    return STATUS_OK;
}
```