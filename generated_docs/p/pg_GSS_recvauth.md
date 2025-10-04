# pg_GSS_recvauth

## Location
[src/backend/libpq/auth.c:928-1080](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/auth.c#L928-L1080)

## Overview
pg_GSS_recvauth implements the server-side GSS-API (Generic Security Services) authentication protocol for PostgreSQL, handling the complete GSSAPI token exchange with clients.

## Definition

```c
static int
pg_GSS_recvauth(Port *port)
```
## Detailed Description
pg_GSS_recvauth is the core function for server-side GSSAPI authentication in PostgreSQL. It establishes a GSS security context by exchanging GSSAPI tokens with the client through a multi-round protocol. The function handles Kerberos keytab configuration, manages the complete token exchange loop, processes delegated credentials if configured, and performs all necessary GSSAPI security context operations. It accepts any service principal present in the server's keytab for maximum interoperability between different Kerberos implementations.

## Parameters / Member Variables
- `*port`: Connection port structure containing client connection information and GSS-specific authentication state
## Dependencies
- Functions called/Symbols referenced:
  - setenv (configures KRB5_KTNAME environment variable for keytab)
  - [pq_startmsgread](pq_startmsgread.md), pq_getbyte, pq_getmessage (protocol message handling)
  - gss_accept_sec_context (core GSSAPI context establishment)
  - gss_release_buffer, gss_release_cred, gss_delete_sec_context (GSSAPI cleanup)
  - [pg_store_delegated_credential](pg_store_delegated_credential.md) (stores delegated credentials if enabled)
  - [sendAuthRequest](../s/sendAuthRequest.md) (sends AUTH_REQ_GSS_CONT continuation requests)
  - [pg_GSS_error](pg_GSS_error.md) (error reporting)
  - [pg_GSS_checkauth](pg_GSS_checkauth.md) (final authentication validation)
- Called from (representative examples):
  - [ClientAuthentication](../C/ClientAuthentication.md) function in auth.c:570

## Notes and Other Information
- Supports multi-round GSSAPI token exchange as required by the protocol
- Configures Kerberos keytab via KRB5_KTNAME environment variable if pg_krb_server_keyfile is set
- Accepts any service principal in keytab for interoperability
- Handles credential delegation when pg_gss_accept_delegation is enabled
- Returns STATUS_ERROR on protocol violations or GSSAPI failures
- Performs proper cleanup of GSSAPI resources (buffers, credentials, contexts)
- Delegates final authentication checks to pg_GSS_checkauth
- Expects PqMsg_GSSResponse message types from client during token exchange
- Maximum token length limited by PG_MAX_AUTH_TOKEN_LENGTH

## Simplified Source

```c
static int
pg_GSS_recvauth(Port *port)
{
    OM_uint32 maj_stat, min_stat, lmin_s, gflags;
    int mtype;
    StringInfoData buf;
    gss_buffer_desc gbuf;
    gss_cred_id_t delegated_creds;

    // Configure Kerberos keytab if specified
    if (pg_krb_server_keyfile != NULL && pg_krb_server_keyfile[0] != '\0') {
        if (setenv("KRB5_KTNAME", pg_krb_server_keyfile, 1) != 0) {
            ereport(FATAL, (errcode(ERRCODE_OUT_OF_MEMORY),
                           errmsg("could not set environment: %m")));
        }
    }

    // Initialize GSS context
    port->gss->cred = GSS_C_NO_CREDENTIAL;
    port->gss->ctx = GSS_C_NO_CONTEXT;
    delegated_creds = GSS_C_NO_CREDENTIAL;
    port->gss->delegated_creds = false;

    // GSSAPI token exchange loop
    do {
        pq_startmsgread();
        CHECK_FOR_INTERRUPTS();

        // Expect GSS response message
        mtype = pq_getbyte();
        if (mtype != PqMsg_GSSResponse) {
            if (mtype != EOF) {
                ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
                               errmsg("expected GSS response, got message type %d", mtype)));
            }
            return STATUS_ERROR;
        }

        // Get GSS token from client
        initStringInfo(&buf);
        if (pq_getmessage(&buf, PG_MAX_AUTH_TOKEN_LENGTH)) {
            pfree(buf.data);
            return STATUS_ERROR;
        }

        gbuf.length = buf.len;
        gbuf.value = buf.data;

        // Process GSS token
        maj_stat = gss_accept_sec_context(&min_stat, &port->gss->ctx,
                                          port->gss->cred, &gbuf,
                                          GSS_C_NO_CHANNEL_BINDINGS,
                                          &port->gss->name, NULL,
                                          &port->gss->outbuf, &gflags,
                                          NULL, pg_gss_accept_delegation ? &delegated_creds : NULL);

        pfree(buf.data);
        CHECK_FOR_INTERRUPTS();

        // Handle delegated credentials if enabled
        if (delegated_creds != GSS_C_NO_CREDENTIAL && gflags & GSS_C_DELEG_FLAG) {
            pg_store_delegated_credential(delegated_creds);
            port->gss->delegated_creds = true;
        }

        // Send response token if needed
        if (port->gss->outbuf.length != 0) {
            sendAuthRequest(port, AUTH_REQ_GSS_CONT,
                           port->gss->outbuf.value, port->gss->outbuf.length);
            gss_release_buffer(&lmin_s, &port->gss->outbuf);
        }

        // Check for GSS errors
        if (maj_stat != GSS_S_COMPLETE && maj_stat != GSS_S_CONTINUE_NEEDED) {
            gss_delete_sec_context(&lmin_s, &port->gss->ctx, GSS_C_NO_BUFFER);
            pg_GSS_error("accepting GSS security context failed", maj_stat, min_stat);
            return STATUS_ERROR;
        }

    } while (maj_stat == GSS_S_CONTINUE_NEEDED);

    // Clean up and proceed to authorization check
    if (port->gss->cred != GSS_C_NO_CREDENTIAL)
        gss_release_cred(&min_stat, &port->gss->cred);

    return pg_GSS_checkauth(port);
}
```