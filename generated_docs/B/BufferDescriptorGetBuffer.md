# BufferDescriptorGetBuffer

## Location
src/include/storage/buf_internals.h: 331 - 336

## Overview
Converts a buffer descriptor pointer back to its corresponding Buffer identifier by extracting the buffer ID and adding 1 to account for PostgreSQL's 1-based buffer numbering scheme.

## Definition
```c
static inline Buffer BufferDescriptorGetBuffer(const BufferDesc *bdesc)
```

## Detailed Description
This inline function performs the inverse operation of converting a buffer ID to a descriptor - it takes a BufferDesc pointer and returns the corresponding Buffer identifier. The function accesses the buf_id field from the buffer descriptor and adds 1 to convert from the internal 0-based indexing to PostgreSQL's external 1-based Buffer numbering system. This conversion is necessary because Buffer identifiers are 1-based (positive for shared buffers, negative for local buffers, 0 is invalid), while internal array indices are 0-based.

## Parameters / Member Variables
- `bdesc`: A const pointer to a BufferDesc structure from which to extract the Buffer identifier

## Dependencies
- Functions called/Symbols referenced:
  - [BufferDesc](BufferDesc.md) (structure type containing buf_id field)
  - Buffer (typedef for buffer identifiers)
- Called from (representative examples):
  - [PinBufferForBlock](../P/PinBufferForBlock.md)
  - [InvalidateBuffer](../I/InvalidateBuffer.md)
  - [GetVictimBuffer](../G/GetVictimBuffer.md)
  - [ExtendBufferedRelShared](../E/ExtendBufferedRelShared.md)
  - PinBuffer
  - UnpinBuffer
  - StartBufferIO
  - TerminateBufferIO

## Notes and Other Information
- This is an inline function for performance optimization, avoiding function call overhead
- Performs the inverse conversion of buffer descriptor access functions
- Essential for converting internal buffer descriptor pointers back to external Buffer handles
- The +1 offset accounts for PostgreSQL's 1-based buffer numbering (Buffer 1 = buf_id 0)
- Used extensively throughout buffer management code when returning Buffer handles to callers
- Part of the buffer identifier abstraction that hides internal indexing details from external users
- Located in buf_internals.h as a core utility for buffer management operations