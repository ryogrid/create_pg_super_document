# new_variable

## Location
src/interfaces/ecpg/preproc/variable.c: 10 - 24

## Overview
Creates and initializes a new variable structure in the ECPG preprocessor, adding it to the global variable list for tracking during SQL statement processing.

## Definition


## Detailed Description
The  function is a constructor for variable structures in the Embedded SQL C (ECPG) preprocessor. It allocates memory for a new variable structure, initializes its fields with the provided parameters, and adds it to the global linked list of all variables (). This function is essential for managing variable declarations and their associated type information during the preprocessing of embedded SQL statements in C code.

The function uses memory management functions specific to the ECPG preprocessor ( and ) to ensure proper memory allocation and string duplication within the preprocessor's memory context.

## Parameters / Member Variables
- : The name of the variable as a C string
- : Pointer to an ECPGtype structure containing type information for the variable
- : Integer indicating the nesting level of braces where this variable was declared

## Dependencies
- Functions called/Symbols referenced:
  - mm_alloc: Memory allocation function for the preprocessor
  - mm_strdup: String duplication function for the preprocessor
  - ECPGtype: Type structure for ECPG variables
- Called from (representative examples):
  - find_struct_member: When processing struct member variables
  - find_variable: When creating new variable entries during parsing

## Notes and Other Information
- The function maintains a global linked list of variables through the  global variable
- Memory is managed using ECPG-specific allocation functions rather than standard malloc/free
- The brace_level parameter is used to track variable scope and lifetime in nested code blocks
- This function is part of the ECPG preprocessor's variable management system for embedded SQL in C