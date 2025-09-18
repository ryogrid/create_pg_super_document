# padBufferToInt

## Location
src/backend/utils/adt/jsonb_util.c: 1533 - 1553

## Overview
A static utility function that appends null-byte padding to a StringInfo buffer to ensure its length is aligned to integer boundaries, returning the number of padding bytes added.

## Definition


## Detailed Description
The `padBufferToInt` function ensures proper memory alignment for JSONB data structures by padding the buffer to integer boundaries. It calculates the number of bytes needed to align the buffer's current length to the next integer boundary using the `INTALIGN` macro, reserves that space in the buffer, and fills it with null bytes. This alignment is crucial for JSONB format compatibility and efficient memory access patterns. The function uses a simple loop to set padding bytes rather than memset for performance reasons, as the padding length is typically small (0-3 bytes for 4-byte integer alignment).

## Parameters / Member Variables
- `buffer`: A StringInfo structure that needs to be padded to integer alignment

## Dependencies
- Functions called/Symbols referenced:
  - INTALIGN (macro for calculating integer alignment)
  - [reserveFromBuffer](../r/reserveFromBuffer.md)
  - [JsonbContainer](../J/JsonbContainer.md) (referenced in context)
- Called from (representative examples):
  - [convertJsonbArray](../c/convertJsonbArray.md)
  - [convertJsonbObject](../c/convertJsonbObject.md)  
  - [convertJsonbScalar](../c/convertJsonbScalar.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the jsonb_util.c compilation unit
- Returns a short integer representing the number of padding bytes added (typically 0-3 for 4-byte alignment)
- Uses a simple for loop instead of memset because padding length is always small
- Essential for maintaining JSONB format specifications which require integer-aligned data structures
- The padding ensures that subsequent integer reads/writes from the buffer are properly aligned for optimal performance
- Commonly used before finalizing JSONB containers to ensure proper structural alignment
- The function fills padding bytes with null characters ('\0')