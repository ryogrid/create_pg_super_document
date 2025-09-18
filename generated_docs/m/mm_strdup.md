# mm_strdup

## Location
src/interfaces/ecpg/preproc/type.c: 25 - 36

## Overview
A string duplication wrapper function that provides error-checked strdup functionality for the ECPG preprocessor, ensuring program termination on allocation failure.

## Definition


## Detailed Description
The  function serves as a safe wrapper around the standard  function specifically designed for the ECPG (Embedded SQL in C) preprocessor. It creates a duplicate copy of the input string with automatic error checking, ensuring that allocation failures during string duplication are handled gracefully by terminating the program with an appropriate error message. This function is essential for reliable string management throughout the ECPG preprocessing pipeline.

## Parameters / Member Variables
- : The null-terminated string to be duplicated

## Dependencies
- Functions called/Symbols referenced:
  - strdup (standard C library function)
  - mmfatal (ECPG error handling function)
  - OUT_OF_MEMORY (error code constant)
- Called from (representative examples):
  - lookup_descriptor
  - output_get_descr
  - output_set_descr
  - sqlda_variable
  - add_preprocessor_define
  - ECPGmake_struct_member
  - ECPGmake_struct_type
  - ECPGdump_a_type
  - new_variable
  - dump_variables
  - adjust_array

## Notes and Other Information
- This function never returns NULL; it either returns a valid string pointer or terminates the program
- Used extensively throughout the ECPG preprocessor for all string duplication operations
- Provides consistent error handling behavior across the entire ECPG codebase for string operations
- The returned string must be freed using free() when no longer needed
- Located in  at lines 25-36