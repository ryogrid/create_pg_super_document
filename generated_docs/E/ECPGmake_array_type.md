# ECPGmake_array_type

## Location
[src/interfaces/ecpg/preproc/type.c:111-120](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/preproc/type.c#L111-L120)

## Overview
Creates an ECPGtype structure representing an array type with a specified element type and size in the ECPG preprocessor.

## Definition

```c
struct ECPGtype *
ECPGmake_array_type(struct ECPGtype *type, char *size)
```
## Detailed Description
This function creates a new ECPGtype structure to represent an array type. It leverages ECPGmake_simple_type to create the basic structure with ECPGt_array type, then sets the element type to point to the provided type parameter. This creates a type hierarchy where the array type contains a reference to its element type, allowing for nested arrays and complex type structures.

The function is part of the ECPG type system that handles arrays of various data types, including primitive types, structs, and even nested arrays.

## Parameters / Member Variables
- : Pointer to ECPGtype structure representing the element type of the array
- : Character string specifying the array size (can be a constant or expression)

## Dependencies
- Functions called/Symbols referenced:
  - [ECPGmake_simple_type](ECPGmake_simple_type.md)
  - ECPGt_array (enum value)
  - [ECPGtype](ECPGtype.md) (struct type)

- Called from (representative examples):
  - [ECPGstruct_member_dup](ECPGstruct_member_dup.md)
  - [find_struct_member](../f/find_struct_member.md)
  - [find_variable](../f/find_variable.md)

## Notes and Other Information
- The function builds upon ECPGmake_simple_type, reusing its initialization logic but specializing it for array types
- The counter parameter passed to ECPGmake_simple_type is set to 0 for arrays
- The u.element field is used to store the reference to the element type, establishing the type hierarchy
- This function enables the creation of complex nested type structures like arrays of structs or multi-dimensional arrays
- Part of the ECPG (Embedded SQL in C) preprocessor's type management system