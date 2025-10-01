# pg_GSS_write

## Location
[src/interfaces/libpq/fe-secure-gssapi.c:93-265](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-secure-gssapi.c#L93-L265)

## Overview
Writes data to a GSSAPI-encrypted PostgreSQL connection, handling encryption, packetization, and partial transmission retry logic.

## Definition

```c
ssize_t
pg_GSS_write(PGconn *conn, const void *ptr, size_t len)
```
## Detailed Description
This function encrypts and transmits data over a GSSAPI-secured connection. It handles the complete lifecycle of encrypting plaintext data using GSS-API, packaging it into properly formatted packets with length headers, and managing partial writes and retries. The function implements robust error handling and maintains state for partial transmission scenarios where the underlying socket would block.

Key behaviors:
- Encrypts data in chunks up to PqGSSMaxPktSize bytes
- Packages encrypted data with 4-byte network-order length headers
- Maintains transmission state across calls for retry scenarios
- Ensures all-or-nothing semantics for the caller (reports success only when all data is processed)
- Validates confidentiality of encrypted packets

## Parameters / Member Variables
- : PostgreSQL connection object containing GSSAPI context and buffers
- : Pointer to the data buffer to be encrypted and transmitted
- : Number of bytes to encrypt and send

## Dependencies
- Functions called/Symbols referenced:
  - gss_wrap (GSSAPI function for encryption)
  - gss_release_buffer (GSSAPI buffer cleanup)
  - [pqsecure_raw_write](pqsecure_raw_write.md) (low-level socket write function)
  - [pg_GSS_error](pg_GSS_error.md) (error reporting function)
  - pg_hton32 (network byte order conversion)
  - [libpq_gettext](../l/libpq_gettext.md) (internationalization)
- Called from:
  - [pqsecure_write](pqsecure_write.md) (main secure write dispatch function)

## Notes and Other Information
- Uses global state variables PqGSSSendBuffer, PqGSSSendLength, PqGSSSendNext, and PqGSSSendConsumed for managing partial transmissions
- Maximum packet size is limited by PQ_GSS_MAX_PACKET_SIZE constant
- Requires that the GSSAPI transport negotiation be complete before use
- Implements careful retry logic where callers must re-send the exact same data on retryable failures
- Returns the number of input bytes consumed on success, -1 on error with errno set appropriately

## Simplified Source

```c
ssize_t pg_GSS_write(PGconn *conn, const void *ptr, size_t len)
{
    OM_uint32 major, minor;
    gss_buffer_desc input, output = GSS_C_EMPTY_BUFFER;
    ssize_t ret = -1;
    size_t bytes_to_encrypt, bytes_encrypted;
    gss_ctx_id_t gctx = conn->gctx;

    // Validate caller is retransmitting enough data on retry
    if (len < PqGSSSendConsumed) {
        appendPQExpBufferStr(&conn->errorMessage,
                           "GSSAPI caller failed to retransmit all data needing to be retried\n");
        errno = EINVAL;
        return -1;
    }

    // Calculate how much new data to encrypt
    bytes_to_encrypt = len - PqGSSSendConsumed;
    bytes_encrypted = PqGSSSendConsumed;

    // Main encryption and transmission loop
    while (bytes_to_encrypt || PqGSSSendLength) {
        int conf_state = 0;
        uint32 netlen;

        // Send any pending encrypted data first
        if (PqGSSSendLength) {
            ssize_t retval;
            ssize_t amount = PqGSSSendLength - PqGSSSendNext;

            retval = pqsecure_raw_write(conn, PqGSSSendBuffer + PqGSSSendNext, amount);
            if (retval <= 0)
                return retval;

            // Handle partial write
            if (retval < amount) {
                PqGSSSendNext += retval;
                continue;
            }

            // Successfully sent all buffered data
            PqGSSSendLength = PqGSSSendNext = 0;
        }

        // Break if no more data to encrypt
        if (!bytes_to_encrypt)
            break;

        // Determine chunk size (limited by max packet size)
        if (bytes_to_encrypt > PqGSSMaxPktSize)
            input.length = PqGSSMaxPktSize;
        else
            input.length = bytes_to_encrypt;

        input.value = (char *) ptr + bytes_encrypted;

        // Encrypt the data chunk
        output.value = NULL;
        output.length = 0;

        major = gss_wrap(&minor, gctx, 1, GSS_C_QOP_DEFAULT,
                        &input, &conf_state, &output);
        if (major != GSS_S_COMPLETE) {
            pg_GSS_error(libpq_gettext("GSSAPI wrap error"), conn, major, minor);
            errno = EIO;
            goto cleanup;
        }

        // Verify confidentiality and size limits
        if (conf_state == 0) {
            libpq_append_conn_error(conn, "outgoing GSSAPI message would not use confidentiality");
            errno = EIO;
            goto cleanup;
        }

        if (output.length > PQ_GSS_MAX_PACKET_SIZE - sizeof(uint32)) {
            libpq_append_conn_error(conn, "client tried to send oversize GSSAPI packet");
            errno = EIO;
            goto cleanup;
        }

        // Update counters
        bytes_encrypted += input.length;
        bytes_to_encrypt -= input.length;
        PqGSSSendConsumed += input.length;

        // Package encrypted data with length header
        netlen = pg_hton32(output.length);
        memcpy(PqGSSSendBuffer + PqGSSSendLength, &netlen, sizeof(uint32));
        PqGSSSendLength += sizeof(uint32);

        memcpy(PqGSSSendBuffer + PqGSSSendLength, output.value, output.length);
        PqGSSSendLength += output.length;

        // Release GSSAPI allocated buffer
        gss_release_buffer(&minor, &output);
    }

    // All data processed successfully
    Assert(len == PqGSSSendConsumed);
    Assert(len == bytes_encrypted);

    PqGSSSendConsumed = 0;  // Reset for next call
    ret = bytes_encrypted;

cleanup:
    if (output.value != NULL)
        gss_release_buffer(&minor, &output);
    return ret;
}
```