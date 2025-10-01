# HeapTupleSetHintBits

## Location
[src/backend/access/heap/heapam_visibility.c:141-169](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam_visibility.c#L141-L169)

## Overview
Public wrapper function for SetHintBits that provides an exported interface for setting commit/abort hint bits on tuple headers from external modules.

## Definition

```c
void
HeapTupleSetHintBits(HeapTupleHeader tuple, Buffer buffer,
					 uint16 infomask, TransactionId xid)
```
## Detailed Description
HeapTupleSetHintBits serves as the exported version of the static inline SetHintBits function. This separation exists due to C99 standard requirements for inline function implementation - inline functions cannot be easily exported across module boundaries, so this wrapper provides external visibility.

The function is a simple pass-through that directly calls SetHintBits with identical parameters, maintaining the same functionality while providing a stable external API. This design pattern allows internal code to use the optimized inline version while external modules access the functionality through this exported wrapper.

## Parameters / Member Variables
- `tuple`: Pointer to the heap tuple header where hint bits will be set
- `buffer`: Buffer containing the tuple, used for durability checks and marking dirty
- `infomask`: Bitmask specifying which hint bits to set (e.g., HEAP_XMIN_COMMITTED, HEAP_XMAX_COMMITTED)
- `xid`: Transaction ID to check for commit status, or InvalidTransactionId if no check needed

## Dependencies
- Functions called/Symbols referenced:
  - [SetHintBits](../S/SetHintBits.md) (the core implementation)
- Called from (representative examples):
  - [UpdateXmaxHintBits](../U/UpdateXmaxHintBits.md)
  - HeapScanIsValid

## Notes and Other Information
- This function exists solely as a C99-compliant export wrapper for the inline SetHintBits function
- The separation allows for both performance optimization (inline) and external accessibility (exported function)
- All actual functionality is implemented in SetHintBits - this is just a thin wrapper
- Used primarily by external access method code that needs to set hint bits on tuples

## Simplified Source

```c
void
HeapTupleSetHintBits(HeapTupleHeader tuple, Buffer buffer,
                     uint16 infomask, TransactionId xid)
{
    // Simply delegate to the inline SetHintBits function
    SetHintBits(tuple, buffer, infomask, xid);
}
```