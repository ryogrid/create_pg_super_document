# BlockRefTableRead

## Location
[src/common/blkreftable.c:1196-1260](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/blkreftable.c#L1196-L1260)

## Overview
Reads data from a BlockRefTableBuffer with buffering optimization and maintains a running CRC calculation for data integrity verification.

## Definition
```c
static void BlockRefTableRead(BlockRefTableReader *reader, void *data, int length)
```

## Detailed Description
This function implements an efficient buffered read mechanism for block reference table data. It uses a three-tier strategy to optimize performance: first using any available buffered data, then performing direct reads for large requests that exceed the buffer size, and finally refilling the buffer for smaller requests. Throughout all read operations, it maintains a running CRC32C checksum for data integrity verification.

The function handles partial reads by looping until the entire requested length is satisfied. It also includes error handling for unexpected end-of-file conditions, invoking an error callback when reads fail to return expected data.

## Parameters / Member Variables
- `reader`: Pointer to the BlockRefTableReader containing the buffer and callback functions
- `data`: Destination buffer where read data will be stored
- `length`: Number of bytes to read from the input source

## Dependencies
- Functions called/Symbols referenced:
  - [BlockRefTableReader](BlockRefTableReader.md) (structure)
  - [BlockRefTableBuffer](BlockRefTableBuffer.md) (structure)
  - memcpy
  - COMP_CRC32C
  - Min
  - BUFSIZE (constant)
- Called from (representative examples):
  - [BlockRefTableWriter](BlockRefTableWriter.md)
  - [CreateBlockRefTableReader](../C/CreateBlockRefTableReader.md)
  - [BlockRefTableReaderNextRelation](BlockRefTableReaderNextRelation.md)
  - [BlockRefTableReaderGetBlocks](BlockRefTableReaderGetBlocks.md)

## Notes and Other Information
- Static function, only accessible within the blkreftable.c module
- Implements three-tier read strategy: buffered data, direct read for large requests, buffer refill for small requests
- Maintains running CRC32C checksum for all data returned to caller
- Handles partial reads by continuing until full request is satisfied
- Uses callback functions for actual I/O operations and error reporting
- Optimizes performance by avoiding buffer copies for large reads (>= BUFSIZE)
- Includes robust error handling for unexpected end-of-file conditions
- Part of the block reference table reader infrastructure for efficient data access

## Simplified Source

```c
static void
BlockRefTableRead(BlockRefTableReader *reader, void *data, int length)
{
    BlockRefTableBuffer *buffer = &reader->buffer;

    // Loop until entire read request is satisfied
    while (length > 0) {
        if (buffer->cursor < buffer->used) {
            // Use buffered data when available
            int bytes_to_copy = Min(length, buffer->used - buffer->cursor);

            memcpy(data, &buffer->data[buffer->cursor], bytes_to_copy);
            COMP_CRC32C(buffer->crc, &buffer->data[buffer->cursor], bytes_to_copy);

            buffer->cursor += bytes_to_copy;
            data = ((char *) data) + bytes_to_copy;
            length -= bytes_to_copy;
        }
        else if (length >= BUFSIZE) {
            // Direct read for large requests (bypass buffer)
            int bytes_read = buffer->io_callback(buffer->io_callback_arg, data, length);

            if (bytes_read == 0) {
                reader->error_callback(reader->error_callback_arg,
                                     "file \"%s\" ends unexpectedly",
                                     reader->error_filename);
            }

            COMP_CRC32C(buffer->crc, data, bytes_read);
            data = ((char *) data) + bytes_read;
            length -= bytes_read;
        }
        else {
            // Refill buffer for small requests
            buffer->used = buffer->io_callback(buffer->io_callback_arg,
                                             buffer->data, BUFSIZE);
            buffer->cursor = 0;

            if (buffer->used == 0) {
                reader->error_callback(reader->error_callback_arg,
                                     "file \"%s\" ends unexpectedly",
                                     reader->error_filename);
            }
        }
    }
}
```