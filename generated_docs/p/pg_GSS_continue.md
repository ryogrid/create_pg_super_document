# pg_GSS_continue

## Location
[src/interfaces/libpq/fe-auth.c:58-160](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-auth.c#L58-L160)

## Overview
Continues GSS authentication with the next token in a multi-step GSSAPI authentication handshake between the PostgreSQL client and server.

## Definition


## Detailed Description
This function handles the continuation of GSSAPI authentication after the initial startup. It manages the exchange of authentication tokens between the client and server through multiple round trips. The function performs the following key operations:

1. **Token Processing**: Reads incoming authentication tokens from the server (if any) into a GSS buffer
2. **Credential Management**: Checks for credential cache availability and handles credential delegation if enabled
3. **Security Context**: Calls  to advance the authentication state machine
4. **Response Generation**: Sends any generated authentication data back to the server
5. **State Management**: Tracks authentication completion and manages GSS context lifecycle

The function supports both initial calls (no input token) and subsequent calls (with server response tokens) during the multi-step authentication process.

## Parameters / Member Variables
- : PostgreSQL connection structure containing GSS context, credentials, and connection state
- : Length of the incoming authentication token from the server (0 for initial call)

## Dependencies
- Functions called/Symbols referenced:
  -  - Memory allocation for input token buffer
  -  - Reads authentication token from connection buffer
  -  - Checks for available credential cache
  -  - Core GSSAPI function to advance authentication
  -  - Sends response token to server
  -  - Error reporting for GSS failures
  -  - Memory cleanup for GSS buffers
  -  - Cleanup for GSS name objects
  -  - Cleanup for GSS security context
- Called from (representative examples):
  -  - Initial GSS authentication setup
  -  - Main authentication dispatcher

## Notes and Other Information
- This is a static function internal to the libpq authentication module
- Handles memory management carefully with proper cleanup on error paths  
- Supports credential delegation when enabled via connection parameters
- Sets  and  flags upon successful completion
- Uses mutual authentication flag (GSS_C_MUTUAL_FLAG) for enhanced security
- Returns STATUS_OK on success, STATUS_ERROR on failure