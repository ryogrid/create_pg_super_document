# fe_scram_state_enum

## Location
src/interfaces/libpq/fe-auth-scram.c: 49 - 79

## Overview
An enumeration that tracks the state of the SCRAM authentication exchange process in PostgreSQL's libpq client library.

## Definition


## Detailed Description
The  enumeration defines the four distinct states of the SCRAM (Salted Challenge Response Authentication Mechanism) authentication protocol exchange between a PostgreSQL client and server. This enum is used within the  structure to track progress through the multi-step SCRAM authentication handshake.

SCRAM authentication involves several message exchanges:
1. Client sends initial authentication request with client nonce
2. Server responds with salt, server nonce, and iteration count
3. Client sends proof of password knowledge
4. Server sends final verification signature

Each enum value corresponds to a specific phase of this authentication protocol, ensuring that messages are processed in the correct sequence and preventing protocol violations.

## Parameters / Member Variables
- : Initial state before any SCRAM messages have been exchanged
- : State after client has sent the first message containing client nonce
- : State after client has sent the final message containing password proof
- : Final state after successful completion of SCRAM authentication

## Dependencies
- Functions called/Symbols referenced:
  - Used within  struct
  - Referenced by SCRAM state machine functions in 
- Called from (representative examples):
  -  (state machine logic)
  -  (initialization)
  - Various SCRAM message processing functions

## Notes and Other Information
- This enum is specific to the client-side (frontend) SCRAM implementation in libpq
- The state transitions are enforced in  using a switch statement
- Invalid state transitions result in authentication failure
- The enum follows PostgreSQL's naming convention with  prefix for frontend components
- Located in 
- Part of PostgreSQL's implementation of RFC 5802 (SCRAM-SHA-1) and RFC 7677 (SCRAM-SHA-256)