# MemoryChunk

## Location
[src/include/utils/memutils_memorychunk.h:124-132](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/memutils_memorychunk.h#L124-L132)

## Overview
MemoryChunk is a lightweight header structure that PostgreSQL's MemoryContexts use to store metadata for allocated memory chunks, including references to the containing block and additional context-specific information.

## Definition
```c
typedef struct MemoryChunk
{
#ifdef MEMORY_CONTEXT_CHECKING
    Size        requested_size;
#endif

    /* bitfield for storing details about the chunk */
    uint64      hdrmask;        /* must be last */
} MemoryChunk;
```

## Detailed Description
MemoryChunk provides a space-efficient header mechanism that memory context implementations use to track allocated chunks. The structure employs a sophisticated bit-packing scheme within the `hdrmask` field to encode multiple pieces of information in a single 64-bit value.

The `hdrmask` field encodes four distinct pieces of information using carefully allocated bit ranges:
1. **4 bits** for the MemoryContextMethodID (bits 0-3)
2. **1 bit** for the external chunk flag (bit 4)
3. **30 bits** for a context-specific value, typically chunk size (bits 5-34, with bit 34 shared)
4. **30 bits** for the block offset - distance from chunk to containing block (bits 34-63)

The clever design shares one bit between the value and block offset fields (bit 34) since both chunk and block pointers are MAXALIGN'd, guaranteeing the lowest bit of any offset is always zero.

The structure supports two modes:
- **Normal chunks**: Store block offset and a 30-bit value directly in hdrmask
- **External chunks**: For large allocations where 30-bit limits are insufficient, marked with a special magic number and managed separately by the context

When `MEMORY_CONTEXT_CHECKING` is enabled, the structure includes an additional `requested_size` field for debugging purposes, expanding from 8 to 16 bytes.

## Parameters / Member Variables
- `requested_size`: (Debug builds only) The originally requested allocation size, used for memory checking and debugging
- `hdrmask`: A packed 64-bit bitfield containing the MemoryContextMethodID, external flag, context-specific value, and block offset

## Dependencies
- **Types/Constants referenced:**
  - `Size` (from PostgreSQL's type system)
  - `MEMORY_CONTEXT_CHECKING` (compile-time flag)
  - `MEMORYCHUNK_MAX_VALUE` (0x3FFFFFFF)
  - `MEMORYCHUNK_MAX_BLOCKOFFSET` (0x3FFFFFFF)
  - `MemoryContextMethodID` (enumeration for context types)

- **Used extensively by memory contexts:**
  - `AllocSetAlloc`, `AllocSetFree`, `AllocSetRealloc` (AllocSet context)
  - `GenerationAlloc`, `GenerationFree`, `GenerationRealloc` (Generation context)
  - `SlabAlloc`, `SlabFree`, `SlabRealloc` (Slab context)
  - `BumpAllocLarge`, `BumpAllocChunkFromBlock` (Bump context)
  - `AlignedAllocFree`, `AlignedAllocRealloc` (Aligned allocation)
  - `MemoryContextAllocAligned` (Generic aligned allocation)

## Notes and Other Information
- **Alignment requirements**: Both chunk and block pointers must be MAXALIGN'd for the bit-sharing scheme to work
- **Size constraints**: Values and block offsets are limited to 30-bit ranges (≈1GB), with external chunks handling larger cases
- **Magic number validation**: External chunks store `MEMORYCHUNK_MAGIC` (0xB1A8DB858EB6EFBA) for integrity checking
- **Performance optimization**: The compact header minimizes memory overhead while providing necessary metadata
- **Portability**: Uses uint64 for cross-platform compatibility
- **Future extensibility**: The design allows future MemoryContext implementations to use custom headers as long as they end with the required 8-byte pattern with MemoryContextMethodID in the low 4 bits

**Key macros for manipulation:**
- `PointerGetMemoryChunk(p)`: Convert allocated pointer back to MemoryChunk header
- `MemoryChunkGetPointer(c)`: Convert MemoryChunk header to user-visible pointer
- `MemoryChunkSetHdrMask()`: Initialize normal chunk with block reference and value
- `MemoryChunkSetHdrMaskExternal()`: Mark chunk as externally managed
- `MemoryChunkIsExternal()`: Test if chunk is externally managed
- `MemoryChunkGetValue()`: Extract the 30-bit context-specific value
- `MemoryChunkGetBlock()`: Get pointer to containing block