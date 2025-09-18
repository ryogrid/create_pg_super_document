# pg_SASL_continue

## Location
src/interfaces/libpq/fe-auth.c: 628 - 699

## Overview
Exchanges messages during the SASL authentication protocol with the backend server, continuing the authentication handshake after initialization.

## Definition


## Detailed Description
The  function handles the message exchange phase of SASL authentication in PostgreSQL's libpq client library. It processes AuthenticationSASLContinue or AuthenticationSASLFinal messages from the server, reads the challenge data, passes it to the SASL mechanism for processing, and sends back the appropriate response.

The function manages the iterative nature of SASL authentication, where multiple message exchanges may be required before authentication completes. It validates the authentication state transitions and ensures proper handling of the final authentication step.

## Parameters / Member Variables
- : Pointer to the PGconn connection structure containing connection state and SASL configuration
- : Length of the challenge payload received from the server
- : Boolean flag indicating whether this is the final authentication message (AuthenticationSASLFinal vs AuthenticationSASLContinue)

## Dependencies
- Functions called/Symbols referenced:
  - malloc
  - [pqGetnchar](pqGetnchar.md)
  - [libpq_append_conn_error](../l/libpq_append_conn_error.md)
  - [pqPacketSend](pqPacketSend.md)
  - free
- Constants used:
  - STATUS_ERROR
  - STATUS_OK
  - SASL_CONTINUE
  - SASL_FAILED
  - PqMsg_SASLResponse
- Called from:
  - pg_fe_sendauth

## Notes and Other Information
- The function allocates a buffer to read the challenge from the server and ensures it's NULL-terminated for safety
- Validates authentication state consistency by checking that SASL authentication completes when the final message is received
- Handles zero-length SASL responses correctly, as permitted by the SASL specification
- Ensures proper memory management by freeing allocated buffers in all code paths
- The authentication exchange continues until the SASL mechanism reports completion or failure
- Error conditions include memory allocation failures, incomplete authentication on final messages, and missing client responses during ongoing exchanges