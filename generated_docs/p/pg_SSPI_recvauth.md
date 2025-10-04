# pg_SSPI_recvauth

## Location
[src/backend/libpq/auth.c:1206-1492](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/auth.c#L1206-L1492)

## Overview
The main server-side function that handles SSPI (Security Support Provider Interface) authentication for PostgreSQL client connections on Windows platforms.

## Definition
```c
static int pg_SSPI_recvauth(Port *port)
```

## Detailed Description
pg_SSPI_recvauth implements the complete server-side SSPI authentication handshake for PostgreSQL. The function performs a multi-step process:

1. **Credentials Acquisition**: Acquires server credentials using the "negotiate" security package, which allows the server to authenticate incoming client requests.

2. **Token Exchange Loop**: Engages in a potentially multi-round message exchange with the client, where each message contains SSPI security tokens. The exchange continues until authentication is complete or fails.

3. **Security Context Management**: Manages SSPI security contexts throughout the authentication process, including proper cleanup on both success and failure paths.

4. **User Identity Extraction**: Upon successful authentication, extracts the authenticated user's identity from the security token, including both the account name and domain information.

5. **Identity Format Conversion**: Converts the Windows identity into either SAM-compatible format (DOMAIN\username) or Kerberos principal format (username@DOMAIN) based on configuration.

6. **Authorization Checking**: Performs final authorization checks including domain validation and user mapping according to the HBA (Host-Based Authentication) configuration.

The function handles various error conditions gracefully, ensuring proper cleanup of allocated resources and security contexts.

## Parameters / Member Variables
- `*port`: Pointer to the Port structure containing connection information, HBA configuration, and client details
## Dependencies
- Functions called/Symbols referenced:
  - AcquireCredentialsHandle (Windows SSPI API)
  - AcceptSecurityContext (Windows SSPI API)
  - QuerySecurityContextToken (Windows SSPI API)
  - GetTokenInformation (Windows API)
  - LookupAccountSid (Windows API)
  - [pg_SSPI_error](pg_SSPI_error.md)
  - [pg_SSPI_make_upn](pg_SSPI_make_upn.md)
  - [pq_startmsgread](pq_startmsgread.md)
  - [pq_getbyte](pq_getbyte.md)
  - [pq_getmessage](pq_getmessage.md)
  - [sendAuthRequest](../s/sendAuthRequest.md)
  - [set_authn_id](../s/set_authn_id.md)
  - [check_usermap](../c/check_usermap.md)
  - malloc/free
- Called from (representative examples):
  - LDAP_OPT_DIAGNOSTIC_MESSAGE context
  - HOSTNAME_LOOKUP_DETAIL context

## Notes and Other Information
- This function is Windows-specific and only compiled on Windows builds of PostgreSQL
- Uses the "negotiate" security package, which supports both NTLM and Kerberos authentication protocols
- Implements proper resource cleanup with multiple exit paths to prevent memory leaks and handle failures gracefully
- Supports both traditional SAM-compatible identity formats and modern Kerberos principal formats
- The authentication process may involve multiple round-trips between client and server
- Token buffer size is limited by PG_MAX_AUTH_TOKEN_LENGTH for security reasons
- Performs case-insensitive domain/realm comparison for SSPI authentication
- The function returns STATUS_OK on success or STATUS_ERROR on failure
- Includes extensive debug logging at DEBUG4 level for troubleshooting authentication issues

## Simplified Source

```c
// Simplified version of pg_SSPI_recvauth
static int pg_SSPI_recvauth(Port *port) {
    CredHandle sspicred;
    CtxtHandle *sspictx = NULL;
    char accountname[MAXPGPATH];
    char domainname[MAXPGPATH];

    // Step 1: Acquire server credentials for "negotiate" protocol
    if (AcquireCredentialsHandle(NULL, "negotiate", SECPKG_CRED_INBOUND,
                                NULL, NULL, NULL, NULL, &sspicred, NULL) != SEC_E_OK) {
        pg_SSPI_error(ERROR, "could not acquire SSPI credentials", r);
    }

    // Step 2: Exchange authentication tokens with client
    SECURITY_STATUS r;
    do {
        // Read SSPI token from client
        StringInfoData buf;
        if (pq_getbyte() != PqMsg_GSSResponse || pq_getmessage(&buf, PG_MAX_AUTH_TOKEN_LENGTH)) {
            // Cleanup and return error
            if (sspictx) {
                DeleteSecurityContext(sspictx);
                free(sspictx);
            }
            FreeCredentialsHandle(&sspicred);
            return STATUS_ERROR;
        }

        // Process token with SSPI
        SecBufferDesc inbuf, outbuf;
        // ... buffer setup ...

        r = AcceptSecurityContext(&sspicred, sspictx, &inbuf,
                                  ASC_REQ_ALLOCATE_MEMORY, SECURITY_NETWORK_DREP,
                                  &newctx, &outbuf, &contextattr, NULL);

        // Send response token if needed
        if (outbuf.cBuffers > 0 && outbuf.pBuffers[0].cbBuffer > 0) {
            sendAuthRequest(port, AUTH_REQ_GSS_CONT,
                           outbuf.pBuffers[0].pvBuffer, outbuf.pBuffers[0].cbBuffer);
            FreeContextBuffer(outbuf.pBuffers[0].pvBuffer);
        }

        // Update context for next iteration
        if (!sspictx) sspictx = malloc(sizeof(CtxtHandle));
        memcpy(sspictx, &newctx, sizeof(CtxtHandle));

    } while (r == SEC_I_CONTINUE_NEEDED);

    FreeCredentialsHandle(&sspicred);

    // Step 3: Extract user identity from completed authentication
    HANDLE token;
    QuerySecurityContextToken(sspictx, &token);
    DeleteSecurityContext(sspictx);
    free(sspictx);

    // Get user information from token
    TOKEN_USER *tokenuser;
    DWORD retlen;
    GetTokenInformation(token, TokenUser, NULL, 0, &retlen);
    tokenuser = malloc(retlen);
    GetTokenInformation(token, TokenUser, tokenuser, retlen, &retlen);
    CloseHandle(token);

    // Convert SID to account name and domain
    DWORD accountnamesize = sizeof(accountname);
    DWORD domainnamesize = sizeof(domainname);
    LookupAccountSid(NULL, tokenuser->User.Sid, accountname, &accountnamesize,
                     domainname, &domainnamesize, NULL);
    free(tokenuser);

    // Step 4: Format authenticated identity
    char *authn_id;
    if (port->hba->compat_realm) {
        authn_id = psprintf("%s\\%s", domainname, accountname);  // SAM format
    } else {
        // Convert to UPN format if needed
        pg_SSPI_make_upn(accountname, sizeof(accountname),
                         domainname, sizeof(domainname), port->hba->upn_username);
        authn_id = psprintf("%s@%s", accountname, domainname);  // Kerberos format
    }
    set_authn_id(port, authn_id);
    pfree(authn_id);

    // Step 5: Validate domain/realm if configured
    if (port->hba->krb_realm && strlen(port->hba->krb_realm)) {
        if (pg_strcasecmp(port->hba->krb_realm, domainname) != 0) {
            return STATUS_ERROR;
        }
    }

    // Step 6: Check user mapping
    if (port->hba->include_realm) {
        char *namebuf = psprintf("%s@%s", accountname, domainname);
        int retval = check_usermap(port->hba->usermap, port->user_name, namebuf, true);
        pfree(namebuf);
        return retval;
    } else {
        return check_usermap(port->hba->usermap, port->user_name, accountname, true);
    }
}
```