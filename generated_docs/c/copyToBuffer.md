# copyToBuffer

## Location
[src/backend/utils/adt/jsonb_util.c:1510-1518](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb_util.c#L1510-L1518)

## Overview
A static utility function that copies a specified number of bytes from a source data pointer to a previously reserved area within a StringInfo buffer at a given offset.

## Definition

```c
static void
copyToBuffer(StringInfo buffer, int offset, const char *data, int len)
```
## Detailed Description
The  function is a low-level memory copy utility specifically designed for JSONB processing. It performs a direct memory copy operation using the standard C library  function to efficiently transfer data to a predetermined location within a StringInfo buffer. This function is used internally within the JSONB utilities to place data at specific offsets within the buffer structure, particularly during JSONB value conversion and serialization processes.

## Parameters / Member Variables
- : A StringInfo structure representing the target buffer where data will be copied
- : The byte offset within the buffer where the copy operation should begin
- : Pointer to the source data that will be copied
- : The number of bytes to copy from the source to the destination

## Dependencies
- Functions called/Symbols referenced:
  - memcpy (standard C library function)
- Called from (representative examples):
  - [appendToBuffer](../a/appendToBuffer.md)
  - [convertJsonbArray](convertJsonbArray.md)  
  - [convertJsonbObject](convertJsonbObject.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the jsonb_util.c compilation unit
- The function assumes that the target area in the buffer has been properly reserved/allocated beforehand
- No bounds checking is performed - the caller is responsible for ensuring the offset and length are valid
- Used primarily in JSONB serialization operations where precise placement of data within the buffer is required
- The function provides a simple abstraction over memcpy for buffer operations in JSONB processing context

## Simplified Source

```c
static void
copyToBuffer(StringInfo buffer, int offset, const char *data, int len)
{
    // Direct memory copy to buffer at specified offset
    memcpy(buffer->data + offset, data, len);
}
```