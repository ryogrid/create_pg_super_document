# mm_alloc

## Location
src/interfaces/ecpg/preproc/type.c: 13 - 24

## Overview
A memory allocation wrapper function that provides error-checked malloc functionality for the ECPG preprocessor, ensuring program termination on allocation failure.

## Definition


## Detailed Description
The  function serves as a safe wrapper around the standard  function specifically designed for the ECPG (Embedded SQL in C) preprocessor. It performs dynamic memory allocation with automatic error checking, ensuring that allocation failures are handled gracefully by terminating the program with an appropriate error message. This function is critical for maintaining memory management reliability throughout the ECPG preprocessing pipeline.

## Parameters / Member Variables
- : The number of bytes to allocate in memory

## Dependencies
- Functions called/Symbols referenced:
  - malloc (standard C library function)
  - mmfatal (ECPG error handling function)
  - OUT_OF_MEMORY (error code constant)
- Called from (representative examples):
  - push_assignment
  - add_descriptor
  - sqlda_variable
  - add_include_path
  - add_preprocessor_define
  - ECPGmake_struct_member
  - ECPGmake_simple_type
  - ECPGdump_a_simple
  - ECPGdump_a_struct
  - new_variable
  - add_variable_to_head
  - add_variable_to_tail

## Notes and Other Information
- This function never returns NULL; it either returns a valid pointer or terminates the program
- Used extensively throughout the ECPG preprocessor for all dynamic memory allocations
- Provides consistent error handling behavior across the entire ECPG codebase
- Located in  at lines 13-24