# scram_state_enum

## Location
[src/backend/libpq/auth-scram.c:128-170](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/auth-scram.c#L128-L170)

## Overview
scram_state_enum is an enumeration that tracks the state of a SCRAM-SHA-256 authentication exchange between the PostgreSQL server and a client.

## Definition


## Detailed Description
This enumeration defines the three distinct phases of the SCRAM (Salted Challenge Response Authentication Mechanism) authentication process in PostgreSQL. The enum is used internally within the auth-scram.c module to maintain state during the multi-step SASL SCRAM-SHA-256 authentication protocol between client and server. Each value represents a specific stage in the authentication handshake, ensuring that messages are processed in the correct order and that the authentication protocol flows properly from initialization through completion.

The SCRAM authentication follows RFC 5802 specifications and provides a secure password-based authentication mechanism that doesn't transmit passwords in plain text.

## Parameters / Member Variables
- : Initial state when SCRAM authentication begins, before any messages have been exchanged between client and server
- : Intermediate state after the server has sent the salt and iteration count to the client in the server-first-message
- : Final state when the authentication exchange has completed, either successfully or with failure

## Dependencies
- Functions called/Symbols referenced:
  - Used as a member type in the scram_state struct
- Called from (representative examples):
  - [pg_SASL_init](../p/pg_SASL_init.md) (sets initial state to SCRAM_AUTH_INIT)
  - [pg_SASL_continue](../p/pg_SASL_continue.md) (transitions between states during authentication)

## Notes and Other Information
- This enum is private to the auth-scram.c file and is not exposed in any header files
- State transitions follow a strict order: INIT → SALT_SENT → FINISHED
- The enum is used within the scram_state structure at src/backend/libpq/auth-scram.c:132
- Invalid state transitions are caught with Assert() statements to ensure protocol integrity
- The authentication can be marked as 'doomed' at any state if errors occur, but the state enum still tracks the protocol phase