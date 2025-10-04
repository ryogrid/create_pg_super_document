# pam_passwd_conv_proc

## Location
[src/backend/libpq/auth.c:1930-2030](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/auth.c#L1930-L2030)

## Overview
The pam_passwd_conv_proc function serves as a PAM (Pluggable Authentication Modules) conversation callback function that handles authentication prompts and responses during the PAM authentication process in PostgreSQL.

## Definition
```c
static int pam_passwd_conv_proc(int num_msg, PG_PAM_CONST struct pam_message **msg,
                                struct pam_response **resp, void *appdata_ptr)
```

## Detailed Description
This function implements the PAM conversation interface, which is the mechanism by which PAM modules communicate with applications during authentication. The function processes various types of PAM messages including password prompts, error messages, and informational text. When PAM requires a password, the function either uses a pre-supplied password or dynamically requests one from the client. The function handles memory allocation for PAM responses using standard C library functions (not PostgreSQL's palloc) since PAM will free this memory during cleanup.

The implementation includes special handling for Solaris 2.6 where the PAM library doesn't properly pass the appdata_ptr parameter, requiring the use of a global variable fallback.

## Parameters / Member Variables
- `num_msg`: Number of messages in the msg array that need to be processed
- `msg`: Array of PAM message structures containing prompts and information from PAM modules
- `resp`: Output parameter - pointer to allocated array of PAM response structures
- `appdata_ptr`: Application data pointer, typically containing the password string (may be NULL on some platforms)

## Dependencies
- Functions called/Symbols referenced:
  - calloc (standard C library memory allocation)
  - [sendAuthRequest](../s/sendAuthRequest.md) (sends authentication request to client)
  - [recv_password_packet](../r/recv_password_packet.md) (receives password from client)
  - strdup (standard C library string duplication)
  - free (standard C library memory deallocation)
- Called from (representative examples):
  - No direct references found (typically used as callback function pointer in PAM authentication)

## Notes and Other Information
- Uses standard C library memory functions (calloc, free) instead of PostgreSQL's memory management to ensure PAM can properly cleanup allocated memory
- Handles different PAM message types: PAM_PROMPT_ECHO_OFF (password prompts), PAM_ERROR_MSG (error messages), PAM_TEXT_INFO (informational messages)
- Includes workaround for broken PAM implementation on Solaris 2.6
- Can dynamically request passwords from clients if not initially provided
- Sets global flag pam_no_password when client refuses to provide password
- Returns PAM_SUCCESS on successful processing, PAM_CONV_ERR on failure
- Validates message count against PAM_MAX_NUM_MSG to prevent buffer overflows
- Logs errors and unsupported conversation types for debugging purposes

## Simplified Source

```c
// Simplified version of pam_passwd_conv_proc
static int pam_passwd_conv_proc(int num_msg, PG_PAM_CONST struct pam_message **msg,
                                struct pam_response **resp, void *appdata_ptr) {
    const char *passwd;
    struct pam_response *reply;

    // Step 1: Get password from appdata or fallback global
    if (appdata_ptr) {
        passwd = (char *) appdata_ptr;
    } else {
        passwd = pam_passwd;  // Solaris 2.6 workaround
    }

    *resp = NULL;

    // Step 2: Validate message count
    if (num_msg <= 0 || num_msg > PAM_MAX_NUM_MSG) {
        return PAM_CONV_ERR;
    }

    // Step 3: Allocate response array (PAM will free this)
    reply = calloc(num_msg, sizeof(struct pam_response));
    if (!reply) {
        ereport(LOG, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
        return PAM_CONV_ERR;
    }

    // Step 4: Process each PAM message
    for (int i = 0; i < num_msg; i++) {
        switch (msg[i]->msg_style) {
            case PAM_PROMPT_ECHO_OFF:  // Password prompt
                if (strlen(passwd) == 0) {
                    // Request password from client
                    sendAuthRequest(pam_port_cludge, AUTH_REQ_PASSWORD, NULL, 0);
                    passwd = recv_password_packet(pam_port_cludge);
                    if (!passwd) {
                        pam_no_password = true;
                        goto fail;
                    }
                }
                reply[i].resp = strdup(passwd);
                if (!reply[i].resp) goto fail;
                reply[i].resp_retcode = PAM_SUCCESS;
                break;

            case PAM_ERROR_MSG:  // Error message
                ereport(LOG, (errmsg("error from underlying PAM layer: %s", msg[i]->msg)));
                // Fall through

            case PAM_TEXT_INFO:  // Informational message
                reply[i].resp = strdup("");
                if (!reply[i].resp) goto fail;
                reply[i].resp_retcode = PAM_SUCCESS;
                break;

            default:  // Unsupported message type
                ereport(LOG, (errmsg("unsupported PAM conversation %d/\"%s\"",
                                    msg[i]->msg_style,
                                    msg[i]->msg ? msg[i]->msg : "(none)")));
                goto fail;
        }
    }

    *resp = reply;
    return PAM_SUCCESS;

fail:
    // Step 5: Clean up on failure
    for (int i = 0; i < num_msg; i++) {
        free(reply[i].resp);
    }
    free(reply);
    return PAM_CONV_ERR;
}
```