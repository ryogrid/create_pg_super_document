# ECPGmake_simple_type

## Location
[src/interfaces/ecpg/preproc/type.c:96-110](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/preproc/type.c#L96-L110)

## Overview
Creates and initializes a new ECPGtype structure for simple (non-composite) data types in the ECPG preprocessor.

## Definition

```c
struct ECPGtype *
ECPGmake_simple_type(enum ECPGttype type, char *size, int counter)
```
## Detailed Description
This function allocates and initializes a new ECPGtype structure to represent simple data types in the ECPG type system. It sets up the basic type information and initializes composite-type specific fields to NULL since this function handles only simple types. The function uses memory allocation through mm_alloc to create the new type structure.

The created type structure is used throughout the ECPG preprocessor to represent and manipulate type information for embedded SQL variables and expressions.

## Parameters / Member Variables
- : The enumerated type value specifying the kind of simple type (e.g., integer, string, etc.)
- : Character string representing the size specification for the type (can be NULL)
- : Integer counter value, specifically noted as needed for varchar and bytea types

## Dependencies
- Functions called/Symbols referenced:
  - [mm_alloc](../m/mm_alloc.md) (memory allocation function)
  - ECPGtype (struct type)
  - ECPGttype (enum type)

- Called from (representative examples):
  - [ECPGstruct_member_dup](ECPGstruct_member_dup.md)
  - [ECPGmake_array_type](ECPGmake_array_type.md)
  - [ECPGmake_struct_type](ECPGmake_struct_type.md)
  - find_struct_member
  - [find_variable](../f/find_variable.md)

## Notes and Other Information
- This function specifically handles simple types only; composite types like structs, unions, and arrays are handled by other ECPGmake_* functions
- The counter parameter is specifically documented as being needed for varchar and bytea types
- All composite-type specific fields (u.element, type_name, struct_sizeof) are initialized to NULL
- Part of the ECPG (Embedded SQL in C) preprocessor's type management system
- Memory is allocated using mm_alloc, which is likely the ECPG preprocessor's memory management function