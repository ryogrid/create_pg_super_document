# read_server_first_message

## Location
[src/interfaces/libpq/fe-auth-scram.c:602-687](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-auth-scram.c#L602-L687)

## Overview
Parses and validates the first message received from the PostgreSQL server during SCRAM authentication, extracting the server nonce, salt, and iteration count.

## Definition
static bool read_server_first_message(fe_scram_state *state, char *input)

## Detailed Description
This function processes the server-first-message in the SCRAM authentication protocol. It parses the message to extract three critical components: the server nonce (which must contain the client nonce as a prefix), the base64-encoded salt used for key derivation, and the iteration count for PBKDF2. The function performs validation to ensure the server properly incorporated the client nonce and that all parameters are well-formed. This data is essential for the subsequent cryptographic operations in the SCRAM exchange.

## Parameters / Member Variables
- : Pointer to fe_scram_state structure to store extracted authentication parameters
- : The raw server-first-message string received from the PostgreSQL server

## Dependencies
- Functions called/Symbols referenced:
  - [read_attr_value](read_attr_value.md) (parses SCRAM message attributes)
  - [pg_b64_decode](../p/pg_b64_decode.md) (decodes base64-encoded salt)
  - [pg_b64_dec_len](../p/pg_b64_dec_len.md) (calculates decoded length)
  - strdup (duplicates strings)
  - malloc (allocates memory for salt)
  - strtol (converts iteration count string to integer)
  - [libpq_append_conn_error](../l/libpq_append_conn_error.md) (error reporting)
- Called from:
  - [scram_exchange](../s/scram_exchange.md) (main SCRAM authentication handler)

## Notes and Other Information
- Validates that server nonce contains client nonce as prefix (prevents replay attacks)
- Decodes base64-encoded salt into binary format for cryptographic operations
- Ensures iteration count is a positive integer (security requirement)
- Stores server_first_message for later use in authentication verification
- Returns false on any parsing error or validation failure
- Critical security validation - improper nonce handling could allow attacks
- Memory allocated for salt and nonce must be freed when authentication completes

## Simplified Source

```c
static bool
read_server_first_message(fe_scram_state *state, char *input)
{
    PGconn *conn = state->conn;
    char *iterations_str;
    char *endptr;
    char *encoded_salt;
    char *nonce;
    int decoded_salt_len;

    // Save the complete server message for later verification
    state->server_first_message = strdup(input);
    if (state->server_first_message == NULL)
    {
        libpq_append_conn_error(conn, "out of memory");
        return false;
    }

    // Parse nonce (r=<server_nonce>)
    nonce = read_attr_value(&input, 'r', &conn->errorMessage);
    if (nonce == NULL)
        return false;

    // Verify server nonce contains our client nonce as prefix
    if (strlen(nonce) < strlen(state->client_nonce) ||
        memcmp(nonce, state->client_nonce, strlen(state->client_nonce)) != 0)
    {
        libpq_append_conn_error(conn, "invalid SCRAM response (nonce mismatch)");
        return false;
    }

    state->nonce = strdup(nonce);
    if (state->nonce == NULL)
    {
        libpq_append_conn_error(conn, "out of memory");
        return false;
    }

    // Parse and decode salt (s=<base64_salt>)
    encoded_salt = read_attr_value(&input, 's', &conn->errorMessage);
    if (encoded_salt == NULL)
        return false;

    decoded_salt_len = pg_b64_dec_len(strlen(encoded_salt));
    state->salt = malloc(decoded_salt_len);
    if (state->salt == NULL)
    {
        libpq_append_conn_error(conn, "out of memory");
        return false;
    }

    state->saltlen = pg_b64_decode(encoded_salt, strlen(encoded_salt),
                                   state->salt, decoded_salt_len);
    if (state->saltlen < 0)
    {
        libpq_append_conn_error(conn, "malformed SCRAM message (invalid salt)");
        return false;
    }

    // Parse iteration count (i=<iterations>)
    iterations_str = read_attr_value(&input, 'i', &conn->errorMessage);
    if (iterations_str == NULL)
        return false;

    state->iterations = strtol(iterations_str, &endptr, 10);
    if (*endptr != '\0' || state->iterations < 1)
    {
        libpq_append_conn_error(conn, "malformed SCRAM message (invalid iteration count)");
        return false;
    }

    // Ensure no garbage data at end of message
    if (*input != '\0')
        libpq_append_conn_error(conn, "malformed SCRAM message (garbage at end of server-first-message)");

    return true;
}
```