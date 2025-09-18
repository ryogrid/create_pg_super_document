# ECPGmake_struct_type

## Location
[src/interfaces/ecpg/preproc/type.c:121-132](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/preproc/type.c#L121-L132)

## Overview
Creates an ECPGtype structure representing a struct or union type with its member list, type name, and size information in the ECPG preprocessor.

## Definition


## Detailed Description
This function creates a new ECPGtype structure to represent composite types (structs or unions). It builds upon ECPGmake_simple_type to create the base structure, then enhances it with composite-type specific information including a duplicated member list, type name, and size information.

The function ensures that the member list is properly duplicated using ECPGstruct_member_dup, creating a complete deep copy of the structure definition. The type name is also duplicated to ensure memory independence.

## Parameters / Member Variables
- : Pointer to the linked list of struct members that define the structure
- : Enumerated type value (typically ECPGt_struct or ECPGt_union)
- : String containing the name of the struct/union type
- : String representing the sizeof expression for the structure

## Dependencies
- Functions called/Symbols referenced:
  - [ECPGmake_simple_type](ECPGmake_simple_type.md)
  - [ECPGstruct_member_dup](ECPGstruct_member_dup.md)
  - [mm_strdup](../m/mm_strdup.md) (string duplication function)
  - ECPGtype (struct type)
  - ECPGttype (enum type)
  - ECPGstruct_member (struct type)

- Called from (representative examples):
  - [ECPGstruct_member_dup](ECPGstruct_member_dup.md)
  - find_struct_member
  - [find_variable](../f/find_variable.md)

## Notes and Other Information
- The function passes "1" as the size parameter to ECPGmake_simple_type, indicating a single instance of the struct
- The counter parameter is set to 0 when calling ECPGmake_simple_type
- Memory for type_name is allocated using mm_strdup to ensure the string is independently managed
- The struct_sizeof parameter is stored directly without duplication, suggesting it may point to a static string or be managed elsewhere
- This function is crucial for handling user-defined types in embedded SQL programs
- Part of the ECPG (Embedded SQL in C) preprocessor's type management system for complex data structures