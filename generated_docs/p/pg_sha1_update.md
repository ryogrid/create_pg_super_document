# pg_sha1_update

## Location
src/common/sha1.c: 332 - 364

## Overview
Processes input data of arbitrary length by buffering incomplete blocks and calling the SHA-1 compression function for each complete 64-byte block, while maintaining accurate bit counts for the message length.

## Definition


## Detailed Description
The  function handles the incremental processing of message data for SHA-1 hashing. It can be called multiple times with different chunks of data, allowing for streaming hash computation of large messages or messages received in fragments.

The function operates by:

1. **Buffering management**: Maintains a 64-byte internal buffer for incomplete message blocks
2. **Block processing**: When the buffer contains a complete 64-byte block, it calls  to process it through the SHA-1 compression function
3. **Length tracking**: Maintains an accurate count of the total message length in bits for final padding
4. **Efficient copying**: Copies input data in optimal chunks to minimize memory operations

The function handles arbitrary input sizes efficiently, whether smaller or larger than the internal block size, making it suitable for both small messages and streaming large data.

## Parameters / Member Variables
- : Pointer to the SHA-1 context structure containing internal state
- : Pointer to the input data bytes to be processed
- : Number of bytes to process from the input data
- Internal variables:
  - : Current position in the 64-byte message buffer
  - : Available space remaining in current buffer block
  - : Number of bytes to copy in current iteration

## Dependencies
- Functions called/Symbols referenced:
  - : Macro for accessing current buffer position  
  - : Called when a complete 64-byte block is ready for processing
  - : For copying input data to the internal message buffer
  - : The SHA-1 context structure type

- Called from:
  - : Part of the generic cryptographic hash interface
  - Applications performing incremental SHA-1 hash computation

## Notes and Other Information
- This is a public function, part of PostgreSQL's external SHA-1 API
- Can be called multiple times between  and 
- Handles input data of any size efficiently, from single bytes to large buffers
- Updates the bit count (ctx->c.b64[0]) for each byte processed, needed for final padding
- Safe for streaming applications - maintains internal state across multiple calls
- Memory-efficient design minimizes copying overhead for various input sizes
- Does not modify input data - takes const pointer for read-only access