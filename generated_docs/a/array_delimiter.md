# array_delimiter

## Location
src/interfaces/ecpg/ecpglib/data.c: 20 - 32

## Overview
A utility function that determines if a given character is a valid delimiter for a specific array type in PostgreSQL's ECPG (Embedded SQL in C) interface.

## Definition


## Detailed Description
The  function checks whether a character  serves as a delimiter for arrays based on the specified array type. This function is part of PostgreSQL's ECPG library and is used for parsing array data in embedded SQL applications.

The function handles two types of arrays:
- **ECPG_ARRAY_ARRAY**: Uses comma () as the delimiter, following standard SQL array syntax
- **ECPG_ARRAY_VECTOR**: Uses space () as the delimiter for vector-style arrays

The function returns  if the character matches the expected delimiter for the given array type, and  otherwise.

## Parameters / Member Variables
- : An enum of type  that specifies the array format being processed
- : The character to test as a potential delimiter

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
- The function is used internally by ECPG's data parsing routines to correctly identify array element boundaries
- The two array types represent different formatting conventions: comma-separated for standard arrays and space-separated for vector notation