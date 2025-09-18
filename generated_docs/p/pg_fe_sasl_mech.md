# pg_fe_sasl_mech

## Location
src/interfaces/libpq/fe-auth-sasl.h: 42 - 142

## Overview
A structure defining frontend SASL mechanism callbacks used for implementing client-side SASL authentication in PostgreSQL libpq connections.

## Definition
```c
typedef struct pg_fe_sasl_mech
{
    void *(*init) (PGconn *conn, const char *password, const char *mech);
    SASLStatus (*exchange) (void *state, char *input, int inputlen,
                           char **output, int *outputlen);
    bool (*channel_bound) (void *state);
    void (*free) (void *state);
} pg_fe_sasl_mech;
```

## Detailed Description
The `pg_fe_sasl_mech` structure serves as an interface for implementing client-side SASL (Simple Authentication and Security Layer) authentication mechanisms in PostgreSQL. This structure contains four function pointers that define the lifecycle and behavior of a SASL mechanism during authentication exchange with a PostgreSQL server. The structure is used internally by libpq to support various SASL mechanisms like SCRAM-SHA-256.

Each mechanism implementation must provide implementations for all four callbacks, which handle initialization, message exchange, channel binding verification, and cleanup phases of the SASL authentication process.

## Parameters / Member Variables
- `init`: Function pointer that initializes mechanism-specific state for a connection. Takes the connection object, user password, and mechanism name as parameters. Returns an opaque state pointer that will be passed to other callbacks, or NULL on failure.
- `exchange`: Function pointer that handles the core SASL message exchange between client and server. Processes server challenges and generates client responses. Returns a SASLStatus indicating whether the exchange should continue, has completed successfully, or has failed.
- `channel_bound`: Function pointer that verifies if the connection has established channel binding. Returns true if channel binding is active and properly established, false otherwise.
- `free`: Function pointer that releases any resources allocated during the init phase. Called when the connection is dropped, not when authentication completes.

## Dependencies
- Functions called/Symbols referenced:
  - SASLStatus (enum defining possible exchange states)
  - PGconn (PostgreSQL connection structure)
- Called from (representative examples):
  - pg_conn (referenced in libpq connection structure)
  - Various SASL mechanism implementations

## Notes and Other Information
- This structure is part of the internal libpq API and is not exposed to client applications
- The mechanism must handle both client-first and server-first SASL flows appropriately
- The exchange callback is called with NULL input during client-first initial response generation
- Memory management is the responsibility of the mechanism implementation - init allocates, free deallocates
- Channel binding support is optional but recommended for enhanced security
- The structure is defined in `src/interfaces/libpq/fe-auth-sasl.h:42-142`
- See `src/include/libpq/sasl.h` for the corresponding backend counterpart