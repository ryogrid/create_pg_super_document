# scram_free

## Location
[src/interfaces/libpq/fe-auth-scram.c:178-204](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-auth-scram.c#L178-L204)

## Overview
Frees all memory and resources associated with a client-side SCRAM authentication exchange state structure.

## Definition

```c
static void
scram_free(void *opaq)
```
## Detailed Description
This function performs complete cleanup of a fe_scram_state structure by systematically deallocating all dynamically allocated memory used during the SCRAM authentication exchange. It frees various message buffers, cryptographic materials, and state information including the password, SASL mechanism name, client nonce, message components, server responses, salt, and the state structure itself. This ensures proper memory management and prevents memory leaks when a SCRAM authentication session ends, whether successfully or unsuccessfully.

## Parameters / Member Variables
- `*opaq`: Opaque pointer to the fe_scram_state structure to be freed
## Dependencies
- Functions called/Symbols referenced:
  - fe_scram_state (structure)
  - SASLStatus (referenced but not directly used in this function)
  - free() (standard C library function)
- Called from (representative examples):
  - No direct references found (likely called through function pointer or cleanup routine)

## Notes and Other Information
- This is a client-side cleanup function (located in fe-auth-scram.c)
- The function is declared static, indicating it's only used within the fe-auth-scram.c file
- Frees multiple categories of data: credentials, protocol messages, cryptographic materials, and server responses
- Safe to call even if some pointers within the state structure are NULL (free() handles NULL pointers gracefully)
- Should be called whenever a SCRAM authentication session ends to prevent memory leaks
- Part of the libpq client library's memory management for SCRAM authentication
- Located in src/interfaces/libpq/fe-auth-scram.c:178-204

## Simplified Source

```c
static void scram_free(void *opaq) {
    fe_scram_state *state = (fe_scram_state *) opaq;

    // Free credential information
    free(state->password);
    free(state->sasl_mechanism);

    // Free client message components
    free(state->client_nonce);
    free(state->client_first_message_bare);
    free(state->client_final_message_without_proof);

    // Free server's first message components
    free(state->server_first_message);
    free(state->salt);
    free(state->nonce);

    // Free server's final message
    free(state->server_final_message);

    // Free the state structure itself
    free(state);
}
```