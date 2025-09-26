# array_boundary

## Location
src/interfaces/ecpg/ecpglib/data.c: 33 - 45

## Overview
A utility function that determines if a given character marks the boundary (end) for a specific array type in PostgreSQL's ECPG (Embedded SQL in C) interface.

## Definition
```c
static bool array_boundary(enum ARRAY_TYPE isarray, char c)
```

## Detailed Description
The `array_boundary` function checks whether a character `c` marks the end boundary of an array based on the specified array type. This function is part of PostgreSQL's ECPG library and is used for parsing array data in embedded SQL applications to detect when array parsing should terminate.

The function handles two types of arrays:
- **ECPG_ARRAY_ARRAY**: Uses closing brace (`}`) as the boundary marker, following standard SQL array syntax where arrays are enclosed in braces
- **ECPG_ARRAY_VECTOR**: Uses null terminator (`\0`) as the boundary marker for vector-style arrays, indicating the end of the string

The function returns `true` if the character matches the expected boundary marker for the given array type, and `false` otherwise.

## Parameters / Member Variables
- `isarray`: An enum of type `ARRAY_TYPE` that specifies the array format being processed
- `c`: The character to test as a potential boundary marker

## Dependencies
- Functions called/Symbols referenced:
  - ARRAY_TYPE (enum)
  - ECPG_ARRAY_ARRAY (enum constant)
  - ECPG_ARRAY_VECTOR (enum constant)
- Called from (representative examples):
  - garbage_left
  - ecpg_get_data

## Notes and Other Information
- This is a static function, meaning it's only accessible within the same source file (data.c)
- The function works in conjunction with `array_delimiter` to provide complete array parsing logic
- The boundary detection is critical for correctly terminating array element collection during parsing
- The two array types represent different formatting conventions with different termination conditions