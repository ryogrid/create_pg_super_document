# build_client_final_message

## Location
[src/interfaces/libpq/fe-auth-scram.c:450-601](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-auth-scram.c#L450-L601)

## Overview
Constructs the final SCRAM authentication message sent by the client, including channel binding data and the cryptographic client proof to verify client knowledge of the password.

## Definition
static char *build_client_final_message(fe_scram_state *state)

## Detailed Description
This function builds the client-final-message in the SCRAM authentication exchange, which is the second and final message sent by the client to the server. It constructs the message in two parts: first the client-final-message-without-proof (containing channel binding and nonce information), then appends the client proof calculated using the calculate_client_proof function. The function handles channel binding data differently depending on whether SCRAM-SHA-256-PLUS is being used, which requires SSL certificate hash information for enhanced security.

## Parameters / Member Variables
- : Pointer to fe_scram_state structure containing authentication state, connection info, nonce data, and cryptographic parameters

## Dependencies
- Functions called/Symbols referenced:
  - [calculate_client_proof](../c/calculate_client_proof.md) (computes cryptographic proof)
  - [pgtls_get_peer_certificate_hash](../p/pgtls_get_peer_certificate_hash.md) (gets SSL cert hash for channel binding)
  - [pg_b64_encode](../p/pg_b64_encode.md) (base64 encoding for binary data)
  - [initPQExpBuffer](../i/initPQExpBuffer.md)/appendPQExpBuffer* (message construction)
  - [enlargePQExpBuffer](../e/enlargePQExpBuffer.md) (buffer management)
  - [libpq_append_conn_error](../l/libpq_append_conn_error.md) (error reporting)
- Called from:
  - [scram_exchange](../s/scram_exchange.md) (main SCRAM authentication handler)

## Notes and Other Information
- Handles two SCRAM variants: SCRAM-SHA-256 and SCRAM-SHA-256-PLUS
- For SCRAM-SHA-256-PLUS, incorporates SSL certificate hash as channel binding data
- Channel binding flag must be consistent with build_client_first_message
- Stores client-final-message-without-proof separately for server verification later
- The client proof demonstrates knowledge of the password without transmitting it
- Critical security function - proper channel binding prevents man-in-the-middle attacks
- Returns dynamically allocated memory that must be freed by caller

## Simplified Source

```c
static char *
build_client_final_message(fe_scram_state *state)
{
    PQExpBufferData buf;
    PGconn *conn = state->conn;
    uint8 client_proof[SCRAM_MAX_KEY_LEN];
    char *result;
    int encoded_len;
    const char *errstr = NULL;

    initPQExpBuffer(&buf);

    // Build channel binding data based on SCRAM mechanism
    if (strcmp(state->sasl_mechanism, SCRAM_SHA_256_PLUS_NAME) == 0)
    {
#ifdef USE_SSL
        char *cbind_data = NULL;
        size_t cbind_data_len = 0;

        // Get SSL certificate hash for channel binding
        cbind_data = pgtls_get_peer_certificate_hash(state->conn, &cbind_data_len);
        if (cbind_data == NULL)
        {
            termPQExpBuffer(&buf);
            return NULL;
        }

        appendPQExpBufferStr(&buf, "c=");

        // Create channel binding input: "p=tls-server-end-point,," + cert_hash
        size_t cbind_header_len = strlen("p=tls-server-end-point,,");
        size_t cbind_input_len = cbind_header_len + cbind_data_len;
        char *cbind_input = malloc(cbind_input_len);
        if (!cbind_input)
        {
            free(cbind_data);
            goto oom_error;
        }

        memcpy(cbind_input, "p=tls-server-end-point,,", cbind_header_len);
        memcpy(cbind_input + cbind_header_len, cbind_data, cbind_data_len);

        // Base64-encode the channel binding data
        encoded_len = pg_b64_enc_len(cbind_input_len);
        if (!enlargePQExpBuffer(&buf, encoded_len))
        {
            free(cbind_data);
            free(cbind_input);
            goto oom_error;
        }

        encoded_len = pg_b64_encode(cbind_input, cbind_input_len,
                                    buf.data + buf.len, encoded_len);
        if (encoded_len < 0)
        {
            free(cbind_data);
            free(cbind_input);
            termPQExpBuffer(&buf);
            appendPQExpBufferStr(&conn->errorMessage,
                                 "could not encode cbind data for channel binding\n");
            return NULL;
        }
        buf.len += encoded_len;
        buf.data[buf.len] = '\0';

        free(cbind_data);
        free(cbind_input);
#else
        termPQExpBuffer(&buf);
        appendPQExpBufferStr(&conn->errorMessage,
                             "channel binding not supported by this build\n");
        return NULL;
#endif
    }
#ifdef USE_SSL
    else if (conn->channel_binding[0] != 'd' && conn->ssl_in_use)
        appendPQExpBufferStr(&buf, "c=eSws"); // base64 of "y,,"
#endif
    else
        appendPQExpBufferStr(&buf, "c=biws"); // base64 of "n,,"

    if (PQExpBufferDataBroken(buf))
        goto oom_error;

    // Add the combined nonce
    appendPQExpBuffer(&buf, ",r=%s", state->nonce);
    if (PQExpBufferDataBroken(buf))
        goto oom_error;

    // Save message without proof for later verification
    state->client_final_message_without_proof = strdup(buf.data);
    if (state->client_final_message_without_proof == NULL)
        goto oom_error;

    // Calculate and append client proof
    if (!calculate_client_proof(state,
                                state->client_final_message_without_proof,
                                client_proof, &errstr))
    {
        termPQExpBuffer(&buf);
        libpq_append_conn_error(conn, "could not calculate client proof: %s", errstr);
        return NULL;
    }

    appendPQExpBufferStr(&buf, ",p=");
    encoded_len = pg_b64_enc_len(state->key_length);
    if (!enlargePQExpBuffer(&buf, encoded_len))
        goto oom_error;

    encoded_len = pg_b64_encode((char *) client_proof, state->key_length,
                                buf.data + buf.len, encoded_len);
    if (encoded_len < 0)
    {
        termPQExpBuffer(&buf);
        libpq_append_conn_error(conn, "could not encode client proof");
        return NULL;
    }
    buf.len += encoded_len;
    buf.data[buf.len] = '\0';

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