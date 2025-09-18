# ECPGstruct_member_dup

## Location
src/interfaces/ecpg/preproc/type.c: 37 - 76

## Overview
Creates a deep copy of a linked list of struct members, recursively duplicating complex nested types including structs, unions, and arrays.

## Definition


## Detailed Description
This function performs a deep duplication of a linked list of ECPGstruct_member structures. It iterates through the provided member list and creates new copies of each member, handling different type categories appropriately:

- For struct/union types: Creates new struct types by recursively duplicating their member lists
- For array types: Creates new array types, with special handling for arrays containing structs/unions
- For simple types: Creates basic type copies

The function ensures that all nested structures are properly duplicated rather than just copying pointers, preventing issues with shared references in the duplicated structure.

## Parameters / Member Variables
- : Pointer to the first member in the linked list of struct members to be duplicated

## Dependencies
- Functions called/Symbols referenced:
  - [ECPGmake_struct_type](ECPGmake_struct_type.md)
  - [ECPGmake_array_type](ECPGmake_array_type.md)
  - [ECPGmake_simple_type](ECPGmake_simple_type.md)
  - [ECPGmake_struct_member](ECPGmake_struct_member.md)
  - ECPGstruct_member (struct type)
  - ECPGtype (struct type)
  - ECPGt_struct, ECPGt_union, ECPGt_array (enum values)

- Called from (representative examples):
  - [ECPGmake_struct_type](ECPGmake_struct_type.md)

## Notes and Other Information
- This function is part of the ECPG (Embedded SQL in C) preprocessor type system
- The function handles recursive type structures correctly, ensuring proper deep copying
- Memory allocation for new members is handled by the called ECPGmake_* functions
- The function preserves the original linked list structure while creating entirely new instances