# pg_SASL_init

## Location
[src/interfaces/libpq/fe-auth.c:422-627](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-auth.c#L422-L627)

## Overview
Initializes SASL authentication exchange between the PostgreSQL client and server, selecting the appropriate SASL mechanism and preparing the initial authentication response.

## Definition

```c
static int
pg_SASL_init(PGconn *conn, int payloadlen)
```
## Detailed Description
The  function handles the initial phase of SASL (Simple Authentication and Security Layer) authentication in PostgreSQL's libpq client library. It parses the list of SASL authentication mechanisms sent by the server in the AuthenticationSASL message, selects the best supported mechanism based on priority and security requirements, and sends the SASLInitialResponse message back to the server.

The function implements mechanism selection logic that prioritizes SCRAM-SHA-256-PLUS (with channel binding) over SCRAM-SHA-256 when SSL is available and channel binding is not disabled. It validates channel binding requirements and ensures secure authentication practices by preventing downgrade attacks.

## Parameters / Member Variables
- : Pointer to the PGconn connection structure containing connection state and configuration
- : Length of the payload in the AuthenticationSASL message (currently unused in the implementation)

## Dependencies
- Functions called/Symbols referenced:
  - initPQExpBuffer
  - [pqGets](pqGets.md)
  - PQExpBufferDataBroken
  - [libpq_append_conn_error](../l/libpq_append_conn_error.md)
  - [pqPutMsgStart](pqPutMsgStart.md)
  - [pqPuts](pqPuts.md)
  - [pqPutInt](pqPutInt.md)
  - [pqPutnchar](pqPutnchar.md)
  - [pqPutMsgEnd](pqPutMsgEnd.md)
  - [pqFlush](pqFlush.md)
  - termPQExpBuffer
- Constants used:
  - SCRAM_SHA_256_PLUS_NAME
  - SCRAM_SHA_256_NAME
  - PqMsg_SASLInitialResponse
  - STATUS_OK
  - STATUS_ERROR
  - SASL_FAILED
- Called from:
  - pg_fe_sendauth

## Notes and Other Information
- The function prioritizes SCRAM-SHA-256-PLUS over SCRAM-SHA-256 when SSL is available and channel binding is enabled
- Channel binding validation prevents man-in-the-middle attacks by ensuring the authentication is bound to the TLS connection
- The function performs comprehensive error checking for invalid authentication requests, unsupported mechanisms, and missing passwords
- Memory management is handled carefully with proper cleanup in error conditions
- Only supports 'tls-unique' channel binding type currently
- Returns STATUS_OK on success, STATUS_ERROR on failure or out-of-memory conditions