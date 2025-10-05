# do_serialize_binary

## Location
[src/backend/utils/misc/guc.c:6017-6031](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L6017-L6031)

## Overview
Performs binary data copying into a destination buffer while updating the buffer pointer and remaining space counter during GUC state serialization.

## Definition
```c
static void do_serialize_binary(char **destptr, Size *maxbytes, void *val, Size valsize)
```

## Detailed Description
This utility function is the binary counterpart to do_serialize, designed for copying raw binary data during GUC (Grand Unified Configuration) state serialization. Unlike do_serialize which handles formatted string operations, this function performs direct memory copying of binary data structures.

The function provides a simple but safe interface for binary serialization:
- Checks that the data size fits within the available buffer space
- Performs direct memory copy using memcpy
- Updates the destination pointer to point past the copied data
- Decrements the remaining buffer space by the copied size

This function is primarily used for serializing binary metadata associated with GUC variables, such as enumeration values, source line numbers, and other non-string data that needs to be preserved exactly during parallel worker communication.

## Parameters / Member Variables
- `destptr`: Pointer to the destination buffer pointer (updated in-place)
- `maxbytes`: Pointer to remaining buffer space counter (updated in-place)  
- `val`: Pointer to the binary data to be copied
- `valsize`: Size of the binary data in bytes

## Dependencies
- Functions called/Symbols referenced:
  - memcpy (standard C library function)
  - elog (PostgreSQL logging function)
- Called from (representative examples):
  - [serialize_variable](../s/serialize_variable.md) (multiple call sites)

## Notes and Other Information
- Simpler than do_serialize as it handles raw binary data without formatting
- No null termination handling needed since it copies exact byte counts
- Used for serializing GUC metadata like source line numbers and enum values
- Part of the GUC serialization infrastructure alongside do_serialize
- Updates both destptr and maxbytes parameters in-place like its string counterpart
- Essential for maintaining exact binary representation of GUC metadata during parallel processing
- Performs bounds checking to prevent buffer overruns

## Simplified Source

```c
static void do_serialize_binary(char **destptr, Size *maxbytes, void *val, Size valsize)
{
    // Check if binary data fits in remaining buffer space
    if (valsize > *maxbytes)
        elog(ERROR, "not enough space to serialize GUC state");

    // Copy binary data directly
    memcpy(*destptr, val, valsize);

    // Update destination pointer and remaining space
    *destptr += valsize;
    *maxbytes -= valsize;
}
```