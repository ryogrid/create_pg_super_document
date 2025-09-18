# pam_passwd_conv_proc

## Location
src/backend/libpq/auth.c: 1930 - 2030

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
  - sendAuthRequest (sends authentication request to client)
  - recv_password_packet (receives password from client)
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