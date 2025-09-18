# pg_be_sasl_mech

## Location
src/include/libpq/sasl.h: 37 - 130

## Overview
The  structure defines callback functions for implementing SASL (Simple Authentication and Security Layer) mechanisms in PostgreSQL's backend authentication system.

## Definition


## Detailed Description
The  structure serves as a callback interface for implementing backend SASL authentication mechanisms in PostgreSQL. It provides a standardized way to handle the three main phases of SASL authentication:

1. **Mechanism Discovery**: Listing supported SASL mechanisms
2. **Initialization**: Setting up mechanism-specific state for a connection
3. **Message Exchange**: Handling the challenge-response authentication flow

Each SASL mechanism implementation (such as SCRAM-SHA-256) provides concrete implementations of these callbacks. The structure is designed to be passed to  during client authentication once the server has decided which authentication method to use.

The interface supports both client-first and server-first SASL mechanisms and handles the complete authentication exchange until success, failure, or continuation.

## Parameters / Member Variables
- : Function pointer that retrieves the list of SASL mechanism names supported by this implementation
  - Input:  (client Port),  (StringInfo buffer to populate with mechanism names)
  - Each mechanism name is null-terminated in the buffer
- : Function pointer that initializes mechanism-specific state for a connection
  - Input:  (client Port),  (mechanism name in use),  (stored secret for the role, or NULL)
  - Returns: Opaque state pointer passed to other callbacks
  - Must handle NULL shadow_pass securely to avoid username disclosure
- : Function pointer that handles the SASL challenge-response exchange
  - Input:  (opaque mechanism state),  (client response data),  (response length)
  - Output:  (server challenge/outcome),  (challenge length),  (optional log message)
  - Returns: PG_SASL_EXCHANGE_CONTINUE (0), PG_SASL_EXCHANGE_SUCCESS (1), or PG_SASL_EXCHANGE_FAILURE (2)

## Dependencies
- Functions called/Symbols referenced:
  - [Port](../P/Port.md) (from libpq/libpq-be.h)
  - StringInfo (from lib/stringinfo.h)
  - PG_SASL_EXCHANGE_* constants (defined in same header)
- Called from (representative examples):
  - [CheckSASLAuth](../C/CheckSASLAuth.md) (src/backend/libpq/auth-sasl.c:52)
  - Referenced by SCRAM implementation (src/include/libpq/scram.h:25)

## Notes and Other Information
- The structure is defined in src/include/libpq/sasl.h:37-130
- SASL allows embedded nulls in responses, so implementations must check inputlen rather than relying on null termination
- Security consideration: When shadow_pass is NULL but the mechanism requires it, implementations should continue the exchange as if authentication failed to prevent username enumeration
- The exchange function should use palloc() for output buffers
- Output parameters are only meaningful when PG_SASL_EXCHANGE_CONTINUE or PG_SASL_EXCHANGE_SUCCESS is returned
- The logdetail parameter helps server administrators debug authentication issues without exposing sensitive information to clients