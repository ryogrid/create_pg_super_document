# _check

## Location
[src/interfaces/ecpg/ecpglib/misc.c:349-358](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/misc.c#L349-L358)

## Overview
A private static utility function that checks if all bytes in a given memory buffer are set to 0xff (255), used for detecting null indicators in ECPG (Embedded SQL in C for PostgreSQL).

## Definition

```c
static bool
_check(const unsigned char *ptr, int length)
```
## Detailed Description
The  function performs a byte-by-byte inspection of a memory buffer to determine if all bytes contain the value 0xff. This function is part of PostgreSQL's ECPG library's null indicator detection mechanism. It iterates through the buffer from the end to the beginning, checking each byte for the 0xff pattern. If any byte is not 0xff, the function immediately returns false; otherwise, it returns true when all bytes match the expected pattern.

## Parameters / Member Variables
- : Pointer to the unsigned character array to be checked
- : Number of bytes in the buffer to examine

## Dependencies
- Functions called/Symbols referenced:
  - (None - uses only basic C operations)
- Called from (representative examples):
  - [ECPGis_noind_null](../E/ECPGis_noind_null.md) (4 times in src/interfaces/ecpg/ecpglib/misc.c)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the misc.c file
- The function iterates backwards through the buffer (from length-1 to 0) for efficiency
- Returns true only if ALL bytes in the specified range are 0xff
- Used as a helper function in ECPG's null indicator validation logic
- Part of the PostgreSQL ECPG (Embedded SQL in C) interface library

## Simplified Source

```c
static bool _check(const unsigned char *ptr, int length) {
    // Check all bytes from end to beginning
    for (length--; length >= 0; length--) {
        if (ptr[length] != 0xff)
            return false;  // Found a byte that's not 0xff
    }

    return true;  // All bytes are 0xff
}
```