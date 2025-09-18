# ecpg_strndup

## Location
src/interfaces/ecpg/compatlib/informix.c: 179 - 197

## Overview
A static utility function that creates a null-terminated duplicate of a string with a maximum length limit, providing safe string duplication with length constraints.

## Definition


## Detailed Description
The  function is a PostgreSQL ECPG (Embedded SQL in C) compatibility library utility that duplicates a string while enforcing a maximum length constraint. It calculates the minimum between the actual string length and the specified maximum length, then allocates memory for a new string copy. The function ensures proper null-termination and handles memory allocation failures by setting errno to ENOMEM.

This function is part of the Informix compatibility layer in ECPG, providing safe string handling operations that prevent buffer overflows by limiting the copied length.

## Parameters / Member Variables
- : Source string to be duplicated (const char *)
- : Maximum number of characters to copy from the source string (size_t)

## Dependencies
- Functions called/Symbols referenced:
  - malloc
  - strlen (implicit)
  - memcpy (implicit)
- Called from (representative examples):
  - deccvasc

## Notes and Other Information
- Returns a newly allocated string that must be freed by the caller
- Sets errno to ENOMEM on memory allocation failure
- Uses the minimum of the actual string length and the specified maximum length
- Part of the ECPG Informix compatibility library located in src/interfaces/ecpg/compatlib/informix.c
- Static function, only accessible within the same compilation unit
- Handles edge cases where the specified length exceeds the actual string length