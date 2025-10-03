# scram_exchange

## Location
[src/backend/libpq/auth-scram.c:348-471](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/auth-scram.c#L348-L471)

## Overview
Handles the SCRAM authentication message exchange between client and server, processing client messages and generating server responses throughout the authentication flow.

## Definition

```c
static int
scram_exchange(void *opaq, const char *input, int inputlen,
			   char **output, int *outputlen, const char **logdetail)
```
## Detailed Description
This function implements the core SCRAM authentication protocol exchange, managing the multi-step authentication handshake between client and server. It operates as a state machine with three main phases:

1. **SCRAM_AUTH_INIT**: Processes the initial client message, extracts authentication parameters, and sends the server's challenge containing salt and iteration count.

2. **SCRAM_AUTH_SALT_SENT**: Processes the client's final message with authentication proof, verifies the client's identity, and sends the server's final verification message.

The function includes timing-attack resistance by always computing client proofs even for mock authentication scenarios. It handles both successful authentication and various failure cases while maintaining consistent error reporting.

## Parameters / Member Variables
- `*opaq`: Opaque pointer to scram_state structure containing authentication context
- `*input`: SCRAM message from client (null-terminated string)
- `inputlen`: Length of input message (must match strlen(input))
- `**output`: Pointer to store the response message for client (allocated by function)
- `*outputlen`: Pointer to store the length of output message
- `**logdetail`: Optional detailed error information for server logs (not sent to client)
## Dependencies
- Functions called/Symbols referenced:
  - [pstrdup](../p/pstrdup.md)
  - strlen
  - [read_client_first_message](../r/read_client_first_message.md)
  - [read_client_final_message](../r/read_client_final_message.md)
  - [build_server_first_message](../b/build_server_first_message.md)
  - [build_server_final_message](../b/build_server_final_message.md)
  - [verify_final_nonce](../v/verify_final_nonce.md)
  - [verify_client_proof](../v/verify_client_proof.md)
  - ereport/errcode/errmsg/errdetail
  - elog
  - Assert
  - PG_SASL_EXCHANGE_CONTINUE
  - PG_SASL_EXCHANGE_SUCCESS
  - PG_SASL_EXCHANGE_FAILURE
- Called from (representative examples):
  - Referenced as function pointer in pg_be_scram_mech structure
  - Used by SASL authentication framework during client authentication

## Notes and Other Information
- This is a static function used as a callback in the SASL mechanism interface
- Returns SASL exchange result codes (CONTINUE, SUCCESS, FAILURE)
- Implements timing-attack resistance by performing consistent computation paths
- Handles empty initial client response by sending empty challenge
- Validates message length consistency to prevent protocol violations
- The 'doomed' flag mechanism ensures consistent failure timing for mock authentication
- Allocates output messages using PostgreSQL memory management (palloc'd strings)
- State transitions are strictly enforced to prevent protocol violations