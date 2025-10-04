# build_client_first_message

## Location
[src/interfaces/libpq/fe-auth-scram.c:345-449](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-auth-scram.c#L345-L449)

## Overview
Builds the first SCRAM authentication exchange message sent by the client to the PostgreSQL server, including nonce generation and channel binding setup.

## Definition

```c
static char *
build_client_first_message(fe_scram_state *state)
```
## Detailed Description
This function constructs the initial client message in the SCRAM (Salted Challenge Response Authentication Mechanism) authentication protocol. It generates a cryptographically secure random nonce, encodes it using base64, and builds a properly formatted SCRAM client-first message according to RFC 5802. The function handles channel binding information based on the connection's SSL status and the selected SCRAM mechanism variant.

The message format follows the SCRAM specification with a GS2 header for channel binding, followed by the actual authentication data. The function preserves a "bare" version of the client message (without channel binding info) that will be needed for later cryptographic calculations.

## Parameters / Member Variables
- `*state`: Pointer to fe_scram_state structure containing the current SCRAM authentication state, connection information, and mechanism details
## Dependencies
- Functions called/Symbols referenced:
  - [pg_strong_random](../p/pg_strong_random.md) (for secure nonce generation)
  - [pg_b64_encode](../p/pg_b64_encode.md) (for base64 encoding the nonce)
  - malloc (for memory allocation)
  - [initPQExpBuffer](../i/initPQExpBuffer.md)/appendPQExpBuffer* (for message construction)
  - [libpq_append_conn_error](../l/libpq_append_conn_error.md) (for error reporting)
- Called from:
  - [scram_exchange](../s/scram_exchange.md) (main SCRAM authentication handler)

## Notes and Other Information
- Generates a cryptographically secure random nonce of SCRAM_RAW_NONCE_LEN bytes
- Supports both SCRAM-SHA-256 and SCRAM-SHA-256-PLUS mechanisms
- Handles channel binding based on SSL connection status and mechanism type
- The username field is left empty as PostgreSQL uses the value from the startup packet
- Stores both the full message and a "bare" version for later cryptographic operations
- Returns dynamically allocated memory that must be freed by the caller
- Critical security component - proper nonce generation is essential for SCRAM security

## Simplified Source

```c
static char *
build_client_first_message(fe_scram_state *state)
{
    PGconn *conn = state->conn;
    char raw_nonce[SCRAM_RAW_NONCE_LEN + 1];
    char *result;
    int channel_info_len;
    int encoded_len;
    PQExpBufferData buf;

    // Generate cryptographically secure random nonce
    if (!pg_strong_random(raw_nonce, SCRAM_RAW_NONCE_LEN))
    {
        libpq_append_conn_error(conn, "could not generate nonce");
        return NULL;
    }

    // Base64-encode the nonce for ASCII transmission
    encoded_len = pg_b64_enc_len(SCRAM_RAW_NONCE_LEN);
    state->client_nonce = malloc(encoded_len + 1);
    if (state->client_nonce == NULL)
    {
        libpq_append_conn_error(conn, "out of memory");
        return NULL;
    }

    encoded_len = pg_b64_encode(raw_nonce, SCRAM_RAW_NONCE_LEN,
                                state->client_nonce, encoded_len);
    if (encoded_len < 0)
    {
        libpq_append_conn_error(conn, "could not encode nonce");
        return NULL;
    }
    state->client_nonce[encoded_len] = '\0';

    // Build the SCRAM message with GS2 header
    initPQExpBuffer(&buf);

    // Channel binding header based on mechanism and SSL status
    if (strcmp(state->sasl_mechanism, SCRAM_SHA_256_PLUS_NAME) == 0)
    {
        Assert(conn->ssl_in_use);
        appendPQExpBufferStr(&buf, "p=tls-server-end-point");
    }
#ifdef USE_SSL
    else if (conn->channel_binding[0] != 'd' && conn->ssl_in_use)
        appendPQExpBufferChar(&buf, 'y'); // Client supports but server doesn't
#endif
    else
        appendPQExpBufferChar(&buf, 'n'); // No channel binding

    if (PQExpBufferDataBroken(buf))
        goto oom_error;

    channel_info_len = buf.len;

    // Add username (empty) and nonce: ",,n=,r=<nonce>"
    appendPQExpBuffer(&buf, ",,n=,r=%s", state->client_nonce);
    if (PQExpBufferDataBroken(buf))
        goto oom_error;

    // Save bare message (without channel binding) for crypto calculations
    state->client_first_message_bare = strdup(buf.data + channel_info_len + 2);
    if (!state->client_first_message_bare)
        goto oom_error;

    result = strdup(buf.data);
    if (result == NULL)
        goto oom_error;

    termPQExpBuffer(&buf);
    return result;

oom_error:
    termPQExpBuffer(&buf);
    libpq_append_conn_error(conn, "out of memory");
    return NULL;
}
```