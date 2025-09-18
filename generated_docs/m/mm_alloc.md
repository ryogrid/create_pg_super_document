# mm_alloc

## Location
[src/interfaces/ecpg/preproc/type.c:13-24](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/preproc/type.c#L13-L24)

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
  - [push_assignment](../p/push_assignment.md)
  - [add_descriptor](../a/add_descriptor.md)
  - [sqlda_variable](../s/sqlda_variable.md)
  - [add_include_path](../a/add_include_path.md)
  - [add_preprocessor_define](../a/add_preprocessor_define.md)
  - [ECPGmake_struct_member](../E/ECPGmake_struct_member.md)
  - [ECPGmake_simple_type](../E/ECPGmake_simple_type.md)
  - [ECPGdump_a_simple](../E/ECPGdump_a_simple.md)
  - [ECPGdump_a_struct](../E/ECPGdump_a_struct.md)
  - [new_variable](../n/new_variable.md)
  - [add_variable_to_head](../a/add_variable_to_head.md)
  - [add_variable_to_tail](../a/add_variable_to_tail.md)

## Notes and Other Information
- This function never returns NULL; it either returns a valid pointer or terminates the program
- Used extensively throughout the ECPG preprocessor for all dynamic memory allocations
- Provides consistent error handling behavior across the entire ECPG codebase
- Located in  at lines 13-24