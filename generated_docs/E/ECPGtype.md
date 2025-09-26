# ECPGtype

## Location
src/interfaces/ecpg/preproc/type.h: 17 - 66

## Overview
ECPGtype is a comprehensive structure that represents data type information in the ECPG (Embedded SQL in C) preprocessor, supporting both simple and complex types including arrays and structs.

## Definition


## Detailed Description
ECPGtype is the central data structure in the ECPG preprocessor for representing all PostgreSQL-compatible data types that can be used in embedded SQL statements. It provides a unified representation for simple types (int, char, etc.), complex types (structs, unions), and container types (arrays). 

The structure uses a discriminated union approach where the 'type' field (ECPGttype enum) determines how to interpret the union 'u'. For arrays, the union contains a pointer to the element type, while for structs and unions, it contains a pointer to the first member in a linked list of ECPGstruct_member structures.

This type system enables the ECPG preprocessor to perform type checking, generate appropriate C code for data conversion between C and SQL representations, and handle memory allocation for complex data structures.

## Parameters / Member Variables
- : An ECPGttype enumeration value indicating the specific data type (ECPGt_int, ECPGt_struct, ECPGt_array, etc.)
- : String containing the name of struct/union types, or NULL for simple types
- : String representing array size (number of elements) or varchar maximum size
- : String containing the sizeof() expression for struct types, used for memory allocation
- : For array types, points to an ECPGtype describing the array element type
- : For struct/union types, points to the first ECPGstruct_member in the linked list of members
- : Reference counter or usage tracking field

## Dependencies
- Functions called/Symbols referenced:
  - ECPGttype (enumeration for type classification)
  - ECPGstruct_member (for struct/union member representation)
- Called from (representative examples):
  - ECPGmake_simple_type
  - ECPGmake_array_type  
  - ECPGmake_struct_type
  - ECPGdump_a_type
  - ECPGfree_type
  - get_type
  - new_variable

## Notes and Other Information
- The structure supports recursive type definitions through the union field, allowing for arrays of structs or structs containing arrays
- Memory management is handled by ECPGfree_type which recursively frees nested type structures
- The type system covers all PostgreSQL data types including numeric, temporal, binary, and user-defined types
- String fields (type_name, size, struct_sizeof) are dynamically allocated and must be properly freed
- This is a core component of the ECPG type checking and code generation system