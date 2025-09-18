# decode_varbyte

## Location
[src/backend/access/gin/ginpostinglist.c:133-196](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginpostinglist.c#L133-L196)

## Overview
Decodes a variable-length byte encoded integer from a byte buffer, performing the inverse operation of encode_varbyte.

## Definition
```c
static uint64 decode_varbyte(unsigned char **ptr)
```

## Detailed Description
This function decodes a 64-bit unsigned integer that was previously encoded using variable-length byte encoding. It reads bytes sequentially from the buffer, checking the continuation bit (MSB) of each byte to determine if more bytes need to be read. The 7 data bits from each byte are assembled into the final 64-bit value, with proper bit shifting to reconstruct the original number.

The function is optimized for performance by unrolling the loop and handling each byte position explicitly. It can decode values up to 49 bits (7 bytes), which is sufficient for the expected range of values in GIN posting lists.

## Parameters / Member Variables
- `ptr`: A pointer to a pointer to unsigned char buffer containing the encoded bytes; this pointer is advanced past the decoded data

## Dependencies
- Functions called/Symbols referenced:
  - Assert (for validation)
  - [GinPostingList](../G/GinPostingList.md) (referenced in comments/context)
- Called from (representative examples):
  - [ginPostingListDecodeAllSegments](../g/ginPostingListDecodeAllSegments.md)

## Notes and Other Information
- This is a static function used internally for GIN index posting list decompression
- The decoding handles up to 7 bytes (49 bits of data), which covers the practical range needed for posting lists
- The function uses explicit unrolled code for performance rather than a loop
- Each byte contributes 7 bits of data, with the MSB indicating whether more bytes follow
- The 7th byte must not have a continuation bit set (assertion check)
- The function advances the buffer pointer to point to the byte after the decoded integer
- This is the inverse operation of encode_varbyte and must handle the same encoding format