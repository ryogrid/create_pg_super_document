# padBufferToInt

## Location
[src/backend/utils/adt/jsonb_util.c:1533-1553](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb_util.c#L1533-L1553)

## Overview
A static utility function that appends null-byte padding to a StringInfo buffer to ensure its length is aligned to integer boundaries, returning the number of padding bytes added.

## Definition

```c
struct must contain enough information to tell what kind
	 * of value it is.
	 */

	res = (Jsonb *) buffer.data;
```
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

## Simplified Source

```c
static short padBufferToInt(StringInfo buffer) {
    // Calculate padding needed for integer alignment
    int padlen = INTALIGN(buffer->len) - buffer->len;

    // Reserve space for padding bytes
    int offset = reserveFromBuffer(buffer, padlen);

    // Fill padding with null bytes (faster than memset for small amounts)
    for (int p = 0; p < padlen; p++) {
        buffer->data[offset + p] = '\0';
    }

    return padlen;
}
```