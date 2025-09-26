# ECPGtype

## Location
[src/interfaces/ecpg/preproc/type.h:17-66](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/preproc/type.h#L17-L66)

## Overview
ECPGtype is a comprehensive structure that represents data type information in the ECPG (Embedded SQL in C) preprocessor, supporting both simple and complex types including arrays and structs.

## Definition

```c
struct ECPGtype
{
	enum ECPGttype type;
	char	   *type_name;		/* For struct and union types it is the struct
								 * name */
	char	   *size;			/* For array it is the number of elements. For
								 * varchar it is the maxsize of the area. */
	char	   *struct_sizeof;	/* For a struct this is the sizeof() type as
								 * string */
	union
	{
		struct ECPGtype *element;	/* For an array this is the type of the
									 * element */
		struct ECPGstruct_member *members;	/* A pointer to a list of members. */
	}			u;
	int			counter;
};
```
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
  - [ECPGstruct_member](ECPGstruct_member.md) (for struct/union member representation)
- Called from (representative examples):
  - [ECPGmake_simple_type](ECPGmake_simple_type.md)
  - [ECPGmake_array_type](ECPGmake_array_type.md)  
  - [ECPGmake_struct_type](ECPGmake_struct_type.md)
  - [ECPGdump_a_type](ECPGdump_a_type.md)
  - [ECPGfree_type](ECPGfree_type.md)
  - [get_type](../g/get_type.md)
  - [new_variable](../n/new_variable.md)

## Notes and Other Information
- The structure supports recursive type definitions through the union field, allowing for arrays of structs or structs containing arrays
- Memory management is handled by ECPGfree_type which recursively frees nested type structures
- The type system covers all PostgreSQL data types including numeric, temporal, binary, and user-defined types
- [String](../S/String.md) fields (type_name, size, struct_sizeof) are dynamically allocated and must be properly freed
- This is a core component of the ECPG type checking and code generation system