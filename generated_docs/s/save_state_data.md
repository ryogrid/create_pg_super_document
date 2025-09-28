# save_state_data

## Location
[src/backend/access/transam/twophase.c:1021-1048](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/twophase.c#L1021-L1048)

## Overview
Appends a block of data to the two-phase commit state records data structure, managing memory allocation and alignment for persistent storage.

## Definition
static void save_state_data(const void *data, uint32 len)

## Detailed Description
This static function is responsible for accumulating state data that will be written to disk as part of a prepared transaction's state file. It manages a linked list of data chunks (StateFileChunk structures) that collectively store all the state information needed to recover or complete a prepared transaction.

The function handles memory management by allocating new chunks when the current chunk doesn't have sufficient space. Each data block is padded to MAXALIGN boundaries to ensure proper alignment when the data is later read from disk. The function copies the input data, allowing callers to safely modify their data after the call returns.

## Parameters / Member Variables
- `data`: Pointer to the data block to be saved (const void* - data is copied, not referenced)
- `len`: Length of the data block in bytes

## Dependencies
- Functions called/Symbols referenced:
  - [StateFileChunk](../S/StateFileChunk.md)
- Called from (representative examples):
  - [StartPrepare](../S/StartPrepare.md) (multiple calls for different state components)
  - [RegisterTwoPhaseRecord](../R/RegisterTwoPhaseRecord.md)

## Notes and Other Information
- Each data block is padded to MAXALIGN boundaries, which must be accounted for when reading the file later
- Uses a chunked approach to handle arbitrarily large state data without requiring contiguous memory
- Allocates chunks in increments of at least 512 bytes or the required padded length, whichever is larger
- Maintains running totals of bytes_free, total_len, and num_chunks for efficient management
- The data is copied into the records structure, so callers retain ownership of their original data
- Critical component of the two-phase commit state persistence mechanism

## Simplified Source

```c
// Simplified version of save_state_data
static void save_state_data(const void *data, uint32 len) {
    uint32 padlen = MAXALIGN(len);

    // Allocate new chunk if current chunk doesn't have enough space
    if (padlen > records.bytes_free) {
        records.tail->next = palloc0(sizeof(StateFileChunk));
        records.tail = records.tail->next;
        records.tail->len = 0;
        records.tail->next = NULL;
        records.num_chunks++;

        // Allocate space (minimum 512 bytes or required padded length)
        records.bytes_free = Max(padlen, 512);
        records.tail->data = palloc(records.bytes_free);
    }

    // Copy data to the chunk and update counters
    memcpy(((char *) records.tail->data) + records.tail->len, data, len);
    records.tail->len += padlen;
    records.bytes_free -= padlen;
    records.total_len += padlen;
}
```

Key simplifications made:
- Removed detailed comments while preserving essential logic
- Simplified variable declarations
- Highlighted the key operations: chunk allocation and data copying
- Preserved MAXALIGN padding critical for disk I/O
- Maintained the chunk-based memory management strategy