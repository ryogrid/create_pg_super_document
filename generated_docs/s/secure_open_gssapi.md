# secure_open_gssapi

## Location
src/backend/libpq/be-secure-gssapi.c: 502 - 740

## Overview
Establishes a GSSAPI-encrypted connection by performing the complete GSSAPI authentication handshake with the client.

## Definition


## Detailed Description
The  function performs the complete GSSAPI authentication and encryption setup process for a PostgreSQL backend connection. This is a blocking function that handles the multi-round handshake protocol required to establish a secure GSSAPI session.

The function operates in several phases:
1. **Initialization**: Allocates GSSAPI state structures and communication buffers
2. **Keytab Setup**: Configures Kerberos keytab file if specified
3. **Authentication Loop**: Exchanges authentication tokens with the client using 
4. **Buffer Management**: Handles reading complete packets from client and sending responses
5. **Finalization**: Resizes buffers for normal operation and determines maximum packet size

The function uses smaller buffers during authentication () then switches to larger buffers () for normal encrypted communication. It handles credential delegation if configured and manages all GSSAPI error conditions.

Unlike the streaming read/write functions, this function will block on socket operations using  to ensure the handshake completes properly.

## Parameters / Member Variables
- : Pointer to Port structure that will be configured with GSSAPI encryption state

## Dependencies
- Functions called/Symbols referenced:
  - : Blocking read helper for complete packet reception
  - : GSSAPI function to process authentication tokens
  - : Determines maximum packet size for encryption
  - : Releases GSSAPI-allocated buffers
  - : PostgreSQL GSSAPI error reporting
  - : Stores delegated Kerberos credentials
  - : Low-level socket write function
  - : Wait for socket readiness
  - : PostgreSQL memory allocation
  - : Sets KRB5_KTNAME environment variable for keytab
- Global buffers managed:
  - , , : Communication buffers
  - , , etc.: Buffer state variables
  - : Maximum packet size for encryption
- Configuration variables:
  - : Path to Kerberos keytab file
  - : Whether to accept delegated credentials
- Called from:
  - : During connection establishment when GSSAPI is negotiated

## Notes and Other Information
- Returns 0 on success, -1 on failure with appropriate error logging
- Sets  to true when encryption is successfully established  
- Allocates  structure in  for connection lifetime
- Handles both regular authentication and credential delegation scenarios
- Enforces packet size limits to prevent memory exhaustion attacks
- The authentication buffer size is smaller than normal operation buffers for efficiency
- Uses network byte order for packet length headers in the protocol
- Function will block until authentication completes or fails, unlike the non-blocking read/write functions
- Supports cleanup of partial state on errors through proper buffer deallocation