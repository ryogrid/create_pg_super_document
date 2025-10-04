# read_server_final_message

## Location
[src/interfaces/libpq/fe-auth-scram.c:688-760](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-auth-scram.c#L688-L760)

## Overview
Parses and validates the final message from the PostgreSQL server in SCRAM authentication, extracting the server signature for mutual authentication verification.

## Definition
static bool read_server_final_message(fe_scram_state *state, char *input)

## Detailed Description
This function processes the server-final-message, which is the last message in the SCRAM authentication exchange. It handles two possible message types: error messages (starting with 'e') that indicate authentication failure, or success messages containing the server signature (attribute 'v'). The server signature proves that the server knows the correct password-derived keys, providing mutual authentication. The function decodes the base64-encoded server signature and stores it in the authentication state for verification against the expected signature calculated by the client.

## Parameters / Member Variables
- : Pointer to fe_scram_state structure to store the server signature
- : The raw server-final-message string received from the PostgreSQL server

## Dependencies
- Functions called/Symbols referenced:
  - [read_attr_value](read_attr_value.md) (parses SCRAM message attributes)
  - [pg_b64_decode](../p/pg_b64_decode.md) (decodes base64-encoded server signature)
  - [pg_b64_dec_len](../p/pg_b64_dec_len.md) (calculates decoded length)
  - strdup (duplicates message string)
  - malloc/free (memory management)
  - memcpy (copies signature data)
  - [libpq_append_conn_error](../l/libpq_append_conn_error.md) (error reporting)
- Called from:
  - [scram_exchange](../s/scram_exchange.md) (main SCRAM authentication handler)

## Notes and Other Information
- Handles both success and error responses from the server
- Server signature length must match the key length for the SCRAM mechanism
- Stores server_final_message for potential debugging or logging purposes
- Critical for mutual authentication - prevents malicious servers from impersonating legitimate ones
- Returns false if message is malformed or contains authentication errors
- The server signature will be compared against client-calculated expected signature
- Memory is allocated temporarily for decoding but freed before function returns

## Simplified Source

```c
static bool
read_server_final_message(fe_scram_state *state, char *input)
{
    PGconn *conn = state->conn;
    char *encoded_server_signature;
    char *decoded_server_signature;
    int server_signature_len;

    // Save the complete server message
    state->server_final_message = strdup(input);
    if (!state->server_final_message)
    {
        libpq_append_conn_error(conn, "out of memory");
        return false;
    }

    // Check for authentication error (e=<error_message>)
    if (*input == 'e')
    {
        char *errmsg = read_attr_value(&input, 'e', &conn->errorMessage);
        if (errmsg == NULL)
            return false;

        libpq_append_conn_error(conn, "error received from server in SCRAM exchange: %s",
                                errmsg);
        return false;
    }

    // Parse server signature (v=<base64_signature>)
    encoded_server_signature = read_attr_value(&input, 'v', &conn->errorMessage);
    if (encoded_server_signature == NULL)
        return false;

    // Ensure no garbage data at end of message
    if (*input != '\0')
        libpq_append_conn_error(conn, "malformed SCRAM message (garbage at end of server-final-message)");

    // Decode server signature from base64
    server_signature_len = pg_b64_dec_len(strlen(encoded_server_signature));
    decoded_server_signature = malloc(server_signature_len);
    if (!decoded_server_signature)
    {
        libpq_append_conn_error(conn, "out of memory");
        return false;
    }

    server_signature_len = pg_b64_decode(encoded_server_signature,
                                         strlen(encoded_server_signature),
                                         decoded_server_signature,
                                         server_signature_len);

    // Validate signature length matches expected key length
    if (server_signature_len != state->key_length)
    {
        free(decoded_server_signature);
        libpq_append_conn_error(conn, "malformed SCRAM message (invalid server signature)");
        return false;
    }

    // Store server signature for mutual authentication verification
    memcpy(state->ServerSignature, decoded_server_signature, state->key_length);
    free(decoded_server_signature);

    return true;
}
```