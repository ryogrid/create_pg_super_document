# rupshift

## Location
src/interfaces/ecpg/compatlib/informix.c: 962 - 969

## Overview
The rupshift function converts all lowercase characters in a string to uppercase characters in-place.

## Definition


## Detailed Description
The rupshift function is part of PostgreSQL's ECPG Informix compatibility library. It iterates through each character in the input string and converts any lowercase letters to their uppercase equivalents using the standard C library functions islower() and toupper(). The conversion is performed in-place, modifying the original string directly.

This function is designed to provide compatibility with Informix database applications that may require string case conversion functionality.

## Parameters / Member Variables
- : A null-terminated character string that will be modified in-place to convert all lowercase characters to uppercase

## Dependencies
- Functions called/Symbols referenced:
  - islower() (standard C library function)
  - toupper() (standard C library function)
- Called from (representative examples):
  - Used in ECPG Informix compatibility context
  - Referenced in test cases for character functions

## Notes and Other Information
- The function modifies the input string in-place, so the original string content is lost
- Uses proper unsigned char casting to handle extended ASCII characters correctly
- Part of the ECPG (Embedded SQL in C for PostgreSQL) Informix compatibility layer
- Located in src/interfaces/ecpg/compatlib/informix.c:962-969