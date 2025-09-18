# CheckSASLAuth

## Location
[src/backend/libpq/auth-sasl.c:52-202](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/auth-sasl.c#L52-L202)

## Overview
CheckSASLAuth performs a complete SASL (Simple Authentication and Security Layer) authentication exchange with a libpq client using a specific SASL mechanism implementation. It manages the multi-message negotiation process between client and server for secure authentication.

## Definition


## Detailed Description
CheckSASLAuth orchestrates a complete SASL authentication session between PostgreSQL server and client. The function implements the server-side SASL protocol handling, which includes:

1. **Mechanism Advertisement**: Sends available SASL mechanisms to the client via AUTH_REQ_SASL
2. **Mechanism Selection**: Processes the client's initial response containing the selected mechanism and optional initial payload
3. **Message Exchange Loop**: Handles the iterative SASL message exchange until authentication succeeds, fails, or continues
4. **Security Considerations**: Implements timing-safe authentication to prevent username enumeration attacks by continuing the SASL exchange even when authentication is "doomed" (invalid user/password)

The function uses the provided SASL mechanism implementation (mech) to handle mechanism-specific logic while managing the PostgreSQL protocol aspects. It supports both mechanisms that use shadow passwords and those that implement alternative authentication schemes.

## Parameters / Member Variables
- : Pointer to a SASL mechanism implementation structure containing function pointers for get_mechanisms, init, and exchange operations
- : PostgreSQL connection port structure containing client connection information and state
- : Optional stored password hash from pg_authid.rolpassword; NULL indicates user not found or mechanism doesn't use passwords
- : Output parameter for detailed error information to assist server administrators with debugging authentication failures

## Dependencies
- Functions called/Symbols referenced:
  - [sendAuthRequest](../s/sendAuthRequest.md) (sends authentication requests to client)
  - [pq_startmsgread](../p/pq_startmsgread.md) (initiates message reading from client)
  - [pq_getbyte](../p/pq_getbyte.md) (reads message type byte)
  - pq_getmessage (reads SASL message payload)
  - [pq_getmsgrawstring](../p/pq_getmsgrawstring.md) (extracts mechanism name from initial response)
  - [pq_getmsgint](../p/pq_getmsgint.md) (reads integer values from messages)
  - [pq_getmsgbytes](../p/pq_getmsgbytes.md) (extracts byte arrays from messages)
  - [pq_getmsgend](../p/pq_getmsgend.md) (finalizes message processing)
  - mech->get_mechanisms (retrieves supported mechanisms)
  - mech->init (initializes mechanism state)
  - mech->exchange (processes SASL exchange messages)
- Message types:
  - PqMsg_SASLResponse
  - AUTH_REQ_SASL
  - AUTH_REQ_SASL_CONT  
  - AUTH_REQ_SASL_FIN
- Status codes:
  - PG_SASL_EXCHANGE_CONTINUE
  - PG_SASL_EXCHANGE_SUCCESS
  - PG_SASL_EXCHANGE_FAILURE
  - STATUS_OK
  - STATUS_ERROR
  - STATUS_EOF
- Called from (representative examples):
  - [CheckPWChallengeAuth](CheckPWChallengeAuth.md)

## Notes and Other Information
- **Security Design**: Implements constant-time authentication behavior to prevent timing attacks that could reveal valid usernames
- **Protocol Compliance**: Enforces SASL protocol rules, such as prohibiting output messages after exchange failure
- **Error Handling**: Distinguishes between client disconnection (EOF) and protocol violations with appropriate error reporting
- **Message Length Limits**: Enforces PG_MAX_SASL_MESSAGE_LENGTH to prevent buffer overflow attacks
- **Debug Support**: Provides DEBUG4-level logging for SASL message exchange debugging
- **Memory Management**: Properly manages StringInfo buffers and mechanism-allocated output throughout the exchange
- **Initial vs Subsequent Messages**: Handles the special format of SASLInitialResponse (mechanism selection + optional payload) differently from subsequent SASLResponse messages