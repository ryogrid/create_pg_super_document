# be_gssapi_read

## Location
[src/backend/libpq/be-secure-gssapi.c:269-429](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/be-secure-gssapi.c#L269-L429)

## Overview
Reads data from a GSSAPI-encrypted connection, handling decryption of incoming packets and buffering of decrypted data for the caller.

## Definition

```c
ssize_t
be_gssapi_read(Port *port, void *ptr, size_t len)
```
## Detailed Description
The  function reads up to  bytes of data from a GSSAPI-encrypted connection into the provided buffer. It operates by:

1. Reading encrypted packets from the network into 
2. Decrypting complete packets using GSSAPI  into 
3. Copying decrypted data from the result buffer to the caller's buffer
4. Managing partial reads and buffering across multiple function calls

The function uses a two-stage buffering approach: it first collects complete encrypted packets (including a 4-byte length prefix), then decrypts them entirely before serving data to callers. This design handles the fact that GSSAPI encryption works on complete packets rather than streaming data.

For non-blocking sockets, the function may return with  if insufficient data is available to complete a packet read or decrypt operation.

## Parameters / Member Variables
- : Pointer to Port structure containing the connection state and GSSAPI context
- : Buffer where decrypted data will be stored
- : Maximum number of bytes to read into the buffer

## Dependencies
- Functions called/Symbols referenced:
  - : Reads raw encrypted data from the underlying socket
  - : GSSAPI function to decrypt and verify integrity of received data
  - : Releases GSSAPI-allocated buffer memory
  - : PostgreSQL function to report GSSAPI errors
  - : Network-to-host byte order conversion for 32-bit integers
- Global buffers used:
  - : Buffer for incoming encrypted packets
  - : Buffer for decrypted data
  - , , : Buffer state variables
- Called from:
  - : Main secure read dispatcher function

## Notes and Other Information
- Requires that GSSAPI transport negotiation has already been completed
- Returns the number of bytes actually read, or -1 on error with errno set
- Uses confidentiality checking to ensure incoming packets were properly encrypted
- Enforces maximum packet size limits () to prevent memory exhaustion attacks
- The function is designed to avoid infinite recursion issues by treating fatal errors consistently
- May return fewer bytes than requested even when more data is available, allowing caller to process data incrementally

## Simplified Source

```c
ssize_t be_gssapi_read(Port *port, void *ptr, size_t len)
{
    OM_uint32 major, minor;
    gss_buffer_desc input, output;
    ssize_t ret;
    size_t bytes_returned = 0;
    gss_ctx_id_t gctx = port->gss->ctx;

    // Read data from GSSAPI-encrypted connection
    while (bytes_returned < len)
    {
        int conf_state = 0;

        // Check if we have buffered decrypted data to return
        if (PqGSSResultNext < PqGSSResultLength)
        {
            size_t bytes_in_buffer = PqGSSResultLength - PqGSSResultNext;
            size_t bytes_to_copy = Min(bytes_in_buffer, len - bytes_returned);

            // Copy data from result buffer to caller's buffer
            memcpy((char *) ptr + bytes_returned,
                   PqGSSResultBuffer + PqGSSResultNext, bytes_to_copy);
            PqGSSResultNext += bytes_to_copy;
            bytes_returned += bytes_to_copy;
            break;
        }

        // Reset buffer pointers when result buffer is empty
        PqGSSResultLength = PqGSSResultNext = 0;
        Assert(bytes_returned == 0);

        // Read packet length (4 bytes) if not already collected
        if (PqGSSRecvLength < sizeof(uint32))
        {
            ret = secure_raw_read(port, PqGSSRecvBuffer + PqGSSRecvLength,
                                  sizeof(uint32) - PqGSSRecvLength);
            if (ret <= 0)
                return ret;

            PqGSSRecvLength += ret;
            if (PqGSSRecvLength < sizeof(uint32))
            {
                errno = EWOULDBLOCK;
                return -1;
            }
        }

        // Decode packet length and validate size
        input.length = pg_ntoh32(*(uint32 *) PqGSSRecvBuffer);
        if (input.length > PQ_GSS_MAX_PACKET_SIZE - sizeof(uint32))
        {
            ereport(COMMERROR,
                    (errmsg("oversize GSSAPI packet sent by the client (%zu > %zu)",
                            (size_t) input.length,
                            PQ_GSS_MAX_PACKET_SIZE - sizeof(uint32))));
            errno = ECONNRESET;
            return -1;
        }

        // Read the encrypted packet data
        ret = secure_raw_read(port, PqGSSRecvBuffer + PqGSSRecvLength,
                              input.length - (PqGSSRecvLength - sizeof(uint32)));
        if (ret <= 0)
            return ret;

        PqGSSRecvLength += ret;

        // Wait for complete packet
        if (PqGSSRecvLength - sizeof(uint32) < input.length)
        {
            errno = EWOULDBLOCK;
            return -1;
        }

        // Decrypt the complete packet
        output.value = NULL;
        output.length = 0;
        input.value = PqGSSRecvBuffer + sizeof(uint32);

        major = gss_unwrap(&minor, gctx, &input, &output, &conf_state, NULL);
        if (major != GSS_S_COMPLETE)
        {
            pg_GSS_error(_("GSSAPI unwrap error"), major, minor);
            errno = ECONNRESET;
            return -1;
        }

        // Verify confidentiality was used
        if (conf_state == 0)
        {
            ereport(COMMERROR,
                    (errmsg("incoming GSSAPI message did not use confidentiality")));
            errno = ECONNRESET;
            return -1;
        }

        // Copy decrypted data to result buffer
        memcpy(PqGSSResultBuffer, output.value, output.length);
        PqGSSResultLength = output.length;

        // Reset receive buffer and release GSSAPI buffer
        PqGSSRecvLength = 0;
        gss_release_buffer(&minor, &output);
    }

    return bytes_returned;
}
```