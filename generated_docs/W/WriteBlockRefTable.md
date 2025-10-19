# WriteBlockRefTable

## Location
[src/common/blkreftable.c:474-576](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/blkreftable.c#L474-L576)

## Overview
Serializes a block reference table to a file by writing its contents in a structured binary format with magic number, entries, chunk data, and CRC checksum.

## Definition

```c
structure. */
	reader = palloc0(sizeof(BlockRefTableReader));
```
## Detailed Description
WriteBlockRefTable converts an in-memory BlockRefTable hash table into a serialized binary format suitable for persistent storage. The function extracts all entries from the hash table, sorts them for consistent ordering, and writes them to a file using the provided callback function. The serialization includes a magic number header, entry metadata, chunk usage arrays, actual block data chunks, and a CRC checksum for integrity verification. The function optimizes storage by trimming trailing zero entries from chunk usage arrays.

## Parameters / Member Variables
- : Pointer to the BlockRefTable containing the hash table of block reference entries to serialize
- : I/O callback function that handles the actual writing of data to the destination
- : Opaque argument passed to the write callback function for context

## Dependencies
- Functions called/Symbols referenced:
  - [BlockRefTableWrite](../B/BlockRefTableWrite.md)
  - [BlockRefTableFileTerminate](../B/BlockRefTableFileTerminate.md)
  - [BlockRefTableComparator](../B/BlockRefTableComparator.md)
  - blockreftable_start_iterate
  - blockreftable_iterate
  - blockreftable_lookup
  - INIT_CRC32C
  - qsort
  - [palloc](../p/palloc.md)
- Called from (representative examples):
  - [SummarizeWAL](../S/SummarizeWAL.md)

## Notes and Other Information
- Writes a BLOCKREFTABLE_MAGIC number as the file header for format identification
- Sorts entries using BlockRefTableComparator for deterministic output ordering
- Optimizes storage by trimming unused trailing chunks from each entry
- Uses CRC32C checksumming for data integrity verification
- Handles empty tables gracefully (only writes magic number and terminator)
- Memory allocation for serialized data is proportional to the number of hash table entries

## Simplified Source

```c
void WriteBlockRefTable(BlockRefTable *brtab,
                        io_callback_fn write_callback,
                        void *write_callback_arg) {
    BlockRefTableSerializedEntry *sdata = NULL;
    BlockRefTableBuffer buffer;
    uint32 magic = BLOCKREFTABLE_MAGIC;

    // Initialize output buffer with callback and CRC
    memset(&buffer, 0, sizeof(BlockRefTableBuffer));
    buffer.io_callback = write_callback;
    buffer.io_callback_arg = write_callback_arg;
    INIT_CRC32C(buffer.crc);

    // Write magic number header
    BlockRefTableWrite(&buffer, &magic, sizeof(uint32));

    // Process entries if table is not empty
    if (brtab->hash->members > 0) {
        unsigned i = 0;
        blockreftable_iterator it;
        BlockRefTableEntry *brtentry;

        // Extract and serialize all entries
        sdata = palloc(brtab->hash->members * sizeof(BlockRefTableSerializedEntry));
        blockreftable_start_iterate(brtab->hash, &it);

        while ((brtentry = blockreftable_iterate(brtab->hash, &it)) != NULL) {
            BlockRefTableSerializedEntry *sentry = &sdata[i++];

            // Copy entry metadata
            sentry->rlocator = brtentry->key.rlocator;
            sentry->forknum = brtentry->key.forknum;
            sentry->limit_block = brtentry->limit_block;
            sentry->nchunks = brtentry->nchunks;

            // Trim trailing zero chunks for storage optimization
            while (sentry->nchunks > 0 &&
                   brtentry->chunk_usage[sentry->nchunks - 1] == 0)
                sentry->nchunks--;
        }

        // Sort entries for deterministic output
        qsort(sdata, brtab->hash->members, sizeof(BlockRefTableSerializedEntry),
              BlockRefTableComparator);

        // Write each sorted entry with its data
        for (i = 0; i < brtab->hash->members; ++i) {
            BlockRefTableSerializedEntry *sentry = &sdata[i];
            BlockRefTableKey key = {{0}};

            // Write entry header
            BlockRefTableWrite(&buffer, sentry, sizeof(BlockRefTableSerializedEntry));

            // Look up original entry for chunk data
            memcpy(&key.rlocator, &sentry->rlocator, sizeof(RelFileLocator));
            key.forknum = sentry->forknum;
            brtentry = blockreftable_lookup(brtab->hash, key);

            // Write chunk usage array
            if (sentry->nchunks != 0)
                BlockRefTableWrite(&buffer, brtentry->chunk_usage,
                                   sentry->nchunks * sizeof(uint16));

            // Write actual chunk data
            for (unsigned j = 0; j < brtentry->nchunks; ++j) {
                if (brtentry->chunk_usage[j] == 0)
                    continue;
                BlockRefTableWrite(&buffer, brtentry->chunk_data[j],
                                   brtentry->chunk_usage[j] * sizeof(uint16));
            }
        }
    }

    // Write terminator and flush buffer
    BlockRefTableFileTerminate(&buffer);
}
```