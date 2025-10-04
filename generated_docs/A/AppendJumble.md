# AppendJumble

## Location
[src/backend/nodes/queryjumblefuncs.c:161-197](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/queryjumblefuncs.c#L161-L197)

## Overview
AppendJumble is a low-level utility function that appends substantive data to the query jumble buffer, handling buffer overflow through hash compression.

## Definition

```c
static void
AppendJumble(JumbleState *jstate, const unsigned char *item, Size size)
```
## Detailed Description
AppendJumble is responsible for adding meaningful query data to the jumble buffer used in query fingerprinting. When the buffer becomes full (reaches JUMBLE_SIZE), it employs a sophisticated compression strategy: it hashes the current buffer contents and replaces the entire buffer with just the hash value, then continues appending new data. This approach ensures that even very large or complex queries can be processed while maintaining a bounded buffer size and preserving the essential characteristics needed for query identification.

## Parameters / Member Variables
- `*jstate`: JumbleState containing the jumble buffer and metadata
- `*item`: Pointer to the data to be appended to the jumble
- `size`: Size in bytes of the data to append
## Dependencies
- Functions called/Symbols referenced:
  - [hash_any_extended](../h/hash_any_extended.md) (computes hash when buffer is full)
  - [DatumGetUInt64](../D/DatumGetUInt64.md) (converts hash result to uint64)
  - JUMBLE_SIZE (constant defining maximum buffer size)
  - memcpy (for copying data to buffer)
- Called from (representative examples):
  - JUMBLE_FIELD (macro for jumbling struct fields)
  - JUMBLE_FIELD_SINGLE (macro for jumbling single fields)
  - JUMBLE_STRING (macro for jumbling string data)

## Notes and Other Information
- Static function (internal to queryjumblefuncs.c)
- Implements intelligent buffer management with hash-based compression
- Handles arbitrary-sized data through chunking when larger than available buffer space
- Critical for maintaining bounded memory usage during query jumbling
- The compression strategy preserves query uniqueness while preventing buffer overflow
- Used indirectly through various JUMBLE_* macros that process different data types
- Part of the core query fingerprinting mechanism in PostgreSQL

## Simplified Source

```c
static void
AppendJumble(JumbleState *jstate, const unsigned char *item, Size size)
{
    unsigned char *jumble = jstate->jumble;
    Size jumble_len = jstate->jumble_len;

    // Process data in chunks, handling buffer overflow
    while (size > 0) {
        // If buffer is full, compress it to a hash
        if (jumble_len >= JUMBLE_SIZE) {
            uint64 hash = DatumGetUInt64(hash_any_extended(jumble, JUMBLE_SIZE, 0));
            memcpy(jumble, &hash, sizeof(hash));
            jumble_len = sizeof(hash);
        }

        // Copy as much data as fits in remaining buffer space
        Size chunk_size = Min(size, JUMBLE_SIZE - jumble_len);
        memcpy(jumble + jumble_len, item, chunk_size);

        // Update pointers and remaining size
        jumble_len += chunk_size;
        item += chunk_size;
        size -= chunk_size;
    }

    jstate->jumble_len = jumble_len;
}
```