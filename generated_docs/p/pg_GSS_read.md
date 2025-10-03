# pg_GSS_read

## Location
[src/interfaces/libpq/fe-secure-gssapi.c:266-434](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-secure-gssapi.c#L266-L434)

## Overview
Reads encrypted data from a GSSAPI-secured PostgreSQL connection, handling packet reception, decryption, and data buffering for the caller.

## Definition

```c
ssize_t
pg_GSS_read(PGconn *conn, void *ptr, size_t len)
```
## Detailed Description
This function receives and decrypts data from a GSSAPI-encrypted connection. It manages the complex process of reading encrypted packets that include length headers, validating packet sizes, decrypting the payload using GSS-API, and buffering the decrypted data for gradual consumption by the caller. The function handles partial packet reception and maintains internal buffers to ensure complete packets are processed atomically.

Key behaviors:
- Reads packets with 4-byte network-order length headers followed by encrypted payload
- Validates packet sizes against PQ_GSS_MAX_PACKET_SIZE limits
- Decrypts complete packets using gss_unwrap
- Maintains result buffer (PqGSSResultBuffer) for decrypted data
- Handles partial reads from both network and result buffer
- Ensures encrypted packets use confidentiality protection

## Parameters / Member Variables
- `*conn`: PostgreSQL connection object containing GSSAPI context and internal buffers
- `*ptr`: Output buffer where decrypted data will be copied
- `len`: Maximum number of bytes to read into the output buffer
## Dependencies
- Functions called/Symbols referenced:
  - gss_unwrap (GSSAPI function for decryption)
  - gss_release_buffer (GSSAPI buffer cleanup)
  - [pqsecure_raw_read](pqsecure_raw_read.md) (low-level socket read function)
  - [pg_GSS_error](pg_GSS_error.md) (error reporting function)
  - pg_ntoh32 (network to host byte order conversion)
  - [libpq_gettext](../l/libpq_gettext.md) (internationalization)
- Called from:
  - [pqsecure_read](pqsecure_read.md) (main secure read dispatch function)

## Notes and Other Information
- Uses global state variables PqGSSRecvBuffer, PqGSSRecvLength for incoming encrypted packets
- Uses PqGSSResultBuffer, PqGSSResultLength, PqGSSResultNext for managing decrypted data
- Implements buffering strategy where partial result data is preserved across function calls
- Returns immediately when any decrypted data is available rather than waiting for complete buffer fill
- Validates that all incoming packets use confidentiality protection
- Sets errno to EWOULDBLOCK for incomplete packet reception scenarios

## Simplified Source

```c
ssize_t pg_GSS_read(PGconn *conn, void *ptr, size_t len)
{
    OM_uint32 major, minor;
    gss_buffer_desc input = GSS_C_EMPTY_BUFFER, output = GSS_C_EMPTY_BUFFER;
    ssize_t ret;
    size_t bytes_returned = 0;
    gss_ctx_id_t gctx = conn->gctx;

    while (bytes_returned < len) {
        int conf_state = 0;

        // Return buffered decrypted data if available
        if (PqGSSResultNext < PqGSSResultLength) {
            size_t bytes_in_buffer = PqGSSResultLength - PqGSSResultNext;
            size_t bytes_to_copy = Min(bytes_in_buffer, len - bytes_returned);

            memcpy((char *) ptr + bytes_returned,
                   PqGSSResultBuffer + PqGSSResultNext, bytes_to_copy);
            PqGSSResultNext += bytes_to_copy;
            bytes_returned += bytes_to_copy;
            break;
        }

        // Reset result buffer when empty
        PqGSSResultLength = PqGSSResultNext = 0;
        Assert(bytes_returned == 0);

        // Read packet length header (4 bytes)
        if (PqGSSRecvLength < sizeof(uint32)) {
            ret = pqsecure_raw_read(conn, PqGSSRecvBuffer + PqGSSRecvLength,
                                  sizeof(uint32) - PqGSSRecvLength);
            if (ret <= 0)
                return ret;

            PqGSSRecvLength += ret;
            if (PqGSSRecvLength < sizeof(uint32)) {
                errno = EWOULDBLOCK;
                return -1;
            }
        }

        // Decode packet length and validate size
        input.length = pg_ntoh32(*(uint32 *) PqGSSRecvBuffer);
        if (input.length > PQ_GSS_MAX_PACKET_SIZE - sizeof(uint32)) {
            libpq_append_conn_error(conn, "oversize GSSAPI packet sent by the server");
            errno = EIO;
            return -1;
        }

        // Read encrypted packet payload
        ret = pqsecure_raw_read(conn, PqGSSRecvBuffer + PqGSSRecvLength,
                              input.length - (PqGSSRecvLength - sizeof(uint32)));
        if (ret <= 0)
            return ret;

        PqGSSRecvLength += ret;
        if (PqGSSRecvLength - sizeof(uint32) < input.length) {
            errno = EWOULDBLOCK;
            return -1;
        }

        // Decrypt complete packet
        input.value = PqGSSRecvBuffer + sizeof(uint32);
        major = gss_unwrap(&minor, gctx, &input, &output, &conf_state, NULL);
        if (major != GSS_S_COMPLETE) {
            pg_GSS_error("GSSAPI unwrap error", conn, major, minor);
            ret = -1;
            errno = EIO;
            goto cleanup;
        }

        // Verify confidentiality was used
        if (conf_state == 0) {
            libpq_append_conn_error(conn, "incoming GSSAPI message did not use confidentiality");
            ret = -1;
            errno = EIO;
            goto cleanup;
        }

        // Copy decrypted data to result buffer
        memcpy(PqGSSResultBuffer, output.value, output.length);
        PqGSSResultLength = output.length;
        PqGSSRecvLength = 0;

        gss_release_buffer(&minor, &output);
    }

    ret = bytes_returned;

cleanup:
    if (output.value != NULL)
        gss_release_buffer(&minor, &output);
    return ret;
}
```