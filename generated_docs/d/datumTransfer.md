# datumTransfer

## Location
[src/backend/utils/adt/datum.c:194-222](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/datum.c#L194-L222)

## Overview
Transfers a non-NULL datum into the current memory context, optimizing for expanded objects by reparenting them rather than flattening like datumCopy does.

## Definition


## Detailed Description
The  function provides an optimized alternative to  when transferring datums into the current memory context. The key optimization occurs with expanded objects:

- **For read-write expanded objects**: Instead of flattening the expanded object (which  would do), this function reparents the expanded object into the current memory context and returns its standard R/W pointer. This preserves the expanded format and avoids the overhead of flattening and re-expanding.
- **For all other datum types**: Falls back to  behavior, creating a new copy in the current memory context.

This function is particularly valuable when working with expanded objects that will continue to be modified, as it maintains their expanded state while ensuring they survive context destruction.

## Parameters / Member Variables
- : The datum value to be transferred into the current memory context
- : Boolean indicating whether the type is passed by value (true) or by reference (false)
- : The declared type length (-1 for varlena, positive for fixed-length)

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetPointer](../D/DatumGetPointer.md)
  - VARATT_IS_EXTERNAL_EXPANDED_RW
  - [TransferExpandedObject](../T/TransferExpandedObject.md)
  - [datumCopy](datumCopy.md)
  - CurrentMemoryContext (global variable)
- Called from (representative examples):
  - [SPI_datumTransfer](../S/SPI_datumTransfer.md)

## Notes and Other Information
- This function assumes the input datum is non-NULL; NULL datums should be handled by the caller
- The optimization only applies to read-write expanded objects (varlena with typLen == -1)
- For read-only expanded objects or other types, the function behaves identically to datumCopy
- The returned pointer may differ from the input pointer even for expanded objects, as TransferExpandedObject returns the standard R/W pointer
- This function is part of the expanded object infrastructure that provides efficient in-memory representations for complex data types
- Primarily used in contexts where datum ownership needs to be transferred between memory contexts while preserving expanded object benefits