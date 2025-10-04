# CheckPAMAuth

## Location
[src/backend/libpq/auth.c:2031-2175](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/auth.c#L2031-L2175)

## Overview
The CheckPAMAuth function implements PAM (Pluggable Authentication Modules) authentication for PostgreSQL, performing the complete PAM authentication sequence to validate user credentials against the configured PAM service.

## Definition
```c
static int CheckPAMAuth(Port *port, const char *user, const char *password)
```

## Detailed Description
This function orchestrates the complete PAM authentication process for PostgreSQL connections. It initializes a PAM session, sets up the conversation interface, configures authentication parameters including the username and remote host information, then executes both authentication (pam_authenticate) and account management (pam_acct_mgmt) phases. The function handles platform-specific PAM quirks, particularly on Solaris 2.6 where appdata_ptr may not work correctly, by using static global variables as fallbacks.

The function supports configurable PAM services through pg_hba.conf and can optionally use hostname resolution for remote connections. It properly manages PAM session lifecycle from initialization through cleanup, ensuring resources are properly released even on failure paths.

## Parameters / Member Variables
- `port`: Pointer to Port structure containing connection information including HBA configuration and remote address details
- `user`: Username string to authenticate with PAM
- `password`: Password string to use for authentication (may be NULL if password will be requested interactively)

## Dependencies
- Functions called/Symbols referenced:
  - pam_start (initializes PAM transaction)
  - pam_set_item (sets PAM authentication items like user, host, conversation)
  - pam_authenticate (performs PAM authentication)
  - pam_acct_mgmt (performs PAM account management checks)
  - pam_end (terminates PAM transaction)
  - pam_strerror (converts PAM error codes to strings)
  - [pg_getnameinfo_all](../p/pg_getnameinfo_all.md) (resolves network addresses to hostnames)
  - unconstify (casts away const qualifier)
  - [set_authn_id](../s/set_authn_id.md) (sets authenticated identity for the connection)
- Called from (representative examples):
  - HOSTNAME_LOOKUP_DETAIL (referenced in auth.c:609)

## Notes and Other Information
- Uses static global variables (pam_passwd, pam_port_cludge) to work around PAM implementation bugs on some platforms
- Supports both default PGSQL_PAM_SERVICE and custom PAM services configured via pg_hba.conf pamservice option
- Handles remote host information setup for non-local connections, with optional hostname resolution controlled by pam_use_hostname setting
- Performs both authentication and account management phases as required by PAM specification
- Returns STATUS_OK for successful authentication, STATUS_ERROR for failures, and STATUS_EOF when client refuses to provide password
- Properly cleans up PAM resources and global state variables on all exit paths
- Sets authenticated identity using set_authn_id only upon successful authentication
- Includes comprehensive error logging with PAM-specific error messages for troubleshooting
- Handles the pam_no_password flag to suppress logging when clients intentionally don't provide passwords

## Simplified Source

```c
// Simplified version of CheckPAMAuth
static int CheckPAMAuth(Port *port, const char *user, const char *password) {
    pam_handle_t *pamh = NULL;
    int retval;

    // Step 1: Set up global variables for conversation callback
    pam_passwd = password;
    pam_port_cludge = port;
    pam_no_password = false;
    pam_passw_conv.appdata_ptr = unconstify(char *, password);

    // Step 2: Initialize PAM session
    const char *service = (port->hba->pamservice && port->hba->pamservice[0] != '\0') ?
                         port->hba->pamservice : PGSQL_PAM_SERVICE;

    retval = pam_start(service, "pgsql@", &pam_passw_conv, &pamh);
    if (retval != PAM_SUCCESS) {
        ereport(LOG, (errmsg("could not create PAM authenticator: %s",
                            pam_strerror(pamh, retval))));
        goto cleanup_and_fail;
    }

    // Step 3: Set PAM user
    retval = pam_set_item(pamh, PAM_USER, user);
    if (retval != PAM_SUCCESS) {
        ereport(LOG, (errmsg("pam_set_item(PAM_USER) failed: %s",
                            pam_strerror(pamh, retval))));
        goto cleanup_and_fail;
    }

    // Step 4: Set remote host information for non-local connections
    if (port->hba->conntype != ctLocal) {
        char hostinfo[NI_MAXHOST];
        int flags = port->hba->pam_use_hostname ? 0 : (NI_NUMERICHOST | NI_NUMERICSERV);

        retval = pg_getnameinfo_all(&port->raddr.addr, port->raddr.salen,
                                   hostinfo, sizeof(hostinfo), NULL, 0, flags);
        if (retval != 0) {
            ereport(WARNING, (errmsg_internal("pg_getnameinfo_all() failed: %s",
                                             gai_strerror(retval))));
            goto cleanup_and_fail;
        }

        retval = pam_set_item(pamh, PAM_RHOST, hostinfo);
        if (retval != PAM_SUCCESS) {
            ereport(LOG, (errmsg("pam_set_item(PAM_RHOST) failed: %s",
                                pam_strerror(pamh, retval))));
            goto cleanup_and_fail;
        }
    }

    // Step 5: Set conversation function
    retval = pam_set_item(pamh, PAM_CONV, &pam_passw_conv);
    if (retval != PAM_SUCCESS) {
        ereport(LOG, (errmsg("pam_set_item(PAM_CONV) failed: %s",
                            pam_strerror(pamh, retval))));
        goto cleanup_and_fail;
    }

    // Step 6: Perform authentication
    retval = pam_authenticate(pamh, 0);
    if (retval != PAM_SUCCESS) {
        if (!pam_no_password) {
            ereport(LOG, (errmsg("pam_authenticate failed: %s",
                                pam_strerror(pamh, retval))));
        }
        pam_passwd = NULL;
        return pam_no_password ? STATUS_EOF : STATUS_ERROR;
    }

    // Step 7: Perform account management check
    retval = pam_acct_mgmt(pamh, 0);
    if (retval != PAM_SUCCESS) {
        if (!pam_no_password) {
            ereport(LOG, (errmsg("pam_acct_mgmt failed: %s",
                                pam_strerror(pamh, retval))));
        }
        pam_passwd = NULL;
        return pam_no_password ? STATUS_EOF : STATUS_ERROR;
    }

    // Step 8: Clean up PAM session
    retval = pam_end(pamh, retval);
    if (retval != PAM_SUCCESS) {
        ereport(LOG, (errmsg("could not release PAM authenticator: %s",
                            pam_strerror(pamh, retval))));
    }

    pam_passwd = NULL;
    set_authn_id(port, user);
    return STATUS_OK;

cleanup_and_fail:
    pam_passwd = NULL;
    return STATUS_ERROR;
}
```