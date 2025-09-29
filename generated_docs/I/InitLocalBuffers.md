# InitLocalBuffers

## Location
[src/backend/storage/buffer/localbuf.c:580-654](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/localbuf.c#L580-L654)

## Overview
Initializes the local buffer cache system by allocating buffer headers, auxiliary arrays, and creating the lookup hash table for temporary table/index buffers.

## Definition
static void InitLocalBuffers(void)

## Detailed Description
This function sets up the local buffer management system that handles temporary tables and indexes. It uses a lazy allocation strategy where actual memory for buffer data pages is allocated only when needed, but buffer headers and management structures are created upfront. The function creates buffer descriptors with negative buffer IDs to distinguish them from shared buffers, sets up reference counting arrays, and creates a hash table for fast buffer lookups.

The function includes safety checks to prevent parallel workers from accessing temporary tables, since they cannot see the local buffers of their leader process. It allocates arrays for buffer descriptors, block pointers, and reference counts, then initializes the buffer IDs with negative values and creates the lookup hash table.

## Parameters / Member Variables
None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - IsParallelWorker
  - calloc
  - [GetLocalBufferDescriptor](../G/GetLocalBufferDescriptor.md)
  - [hash_create](../h/hash_create.md)
- Called from (representative examples):
  - LocalBufHdrGetBlock
  - [PrefetchLocalBuffer](../P/PrefetchLocalBuffer.md)
  - [LocalBufferAlloc](../L/LocalBufferAlloc.md)
  - [ExtendBufferedRelLocal](../E/ExtendBufferedRelLocal.md)

## Notes and Other Information
- Uses lazy allocation - buffer headers are created but actual data page memory allocated on demand
- Prevents parallel workers from accessing temporary tables with explicit error check
- Buffer IDs start at -2 (becoming -1 after BufferDescriptorGetBuffer adjustment) to distinguish from shared buffers
- Creates LocalBufHash hash table for fast buffer tag lookups
- Allocates LocalBufferDescriptors, LocalBufferBlockPointers, and LocalRefCount arrays
- Sets global NLocBuffer to indicate local buffer system is ready
- Uses num_temp_buffers GUC parameter to determine number of local buffers to allocate
- Intentionally leaves atomic variables uninitialized to catch incorrect atomic usage on local buffers

## Simplified Source

```c
static void
InitLocalBuffers(void)
{
    int nbufs = num_temp_buffers;
    HASHCTL info;
    int i;

    // Prevent parallel workers from accessing temporary tables
    if (IsParallelWorker())
        ereport(ERROR, (errcode(ERRCODE_INVALID_TRANSACTION_STATE),
                       errmsg("cannot access temporary tables during a parallel operation")));

    // Allocate buffer management arrays
    LocalBufferDescriptors = calloc(nbufs, sizeof(BufferDesc));
    LocalBufferBlockPointers = calloc(nbufs, sizeof(Block));
    LocalRefCount = calloc(nbufs, sizeof(int32));

    // Check allocation success
    if (!LocalBufferDescriptors || !LocalBufferBlockPointers || !LocalRefCount)
        ereport(FATAL, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));

    nextFreeLocalBufId = 0;

    // Initialize buffer descriptors with negative IDs
    for (i = 0; i < nbufs; i++) {
        BufferDesc *buf = GetLocalBufferDescriptor(i);
        buf->buf_id = -i - 2;  // Negative to distinguish from shared buffers
    }

    // Create hash table for buffer lookups
    info.keysize = sizeof(BufferTag);
    info.entrysize = sizeof(LocalBufferLookupEnt);

    LocalBufHash = hash_create("Local Buffer Lookup Table", nbufs, &info,
                              HASH_ELEM | HASH_BLOBS);

    if (!LocalBufHash)
        elog(ERROR, "could not initialize local buffer hash table");

    // Mark initialization complete
    NLocBuffer = nbufs;
}
```