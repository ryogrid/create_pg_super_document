# pg_SSPI_continue

## Location
src/interfaces/libpq/fe-auth.c: 218 - 350

## Overview
Continues SSPI authentication with the next token in a multi-step Windows authentication handshake between the PostgreSQL client and server.

## Definition


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