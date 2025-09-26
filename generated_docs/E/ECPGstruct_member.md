# ECPGstruct_member

## Location
[src/interfaces/ecpg/preproc/type.h:10-16](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/preproc/type.h#L10-L16)

## Overview
ECPGstruct_member is a struct type that represents a single member within a PostgreSQL ECPG (Embedded SQL in C) struct definition, forming a linked list of struct members.

## Definition

```c
struct ECPGstruct_member
{
	char	   *name;
	struct ECPGtype *type;
	struct ECPGstruct_member *next;
};
```
## Detailed Description
ECPGstruct_member is a fundamental data structure used in the ECPG preprocessor to represent individual members of C struct types that are used in embedded SQL statements. Each instance represents one member of a struct, containing the member's name, its type information, and a pointer to the next member in the struct. This forms a singly-linked list that represents the complete structure definition.

The structure is part of the ECPG type system that allows the preprocessor to understand and validate C data structures used in SQL operations, ensuring proper data type mapping between C and SQL.

## Parameters / Member Variables
- `*name`: A string containing the name of the struct member
- `*type`: A pointer to an ECPGtype structure that describes the data type of this member
- `*next`: A pointer to the next ECPGstruct_member in the linked list, or NULL if this is the last member
## Dependencies
- Functions called/Symbols referenced:
  - [ECPGtype](ECPGtype.md) (referenced as member type)
  - [ECPGstruct_member](ECPGstruct_member.md) (self-reference for linked list)
- Called from (representative examples):
  - [ECPGstruct_member_dup](ECPGstruct_member_dup.md)
  - [ECPGmake_struct_member](ECPGmake_struct_member.md)
  - [ECPGmake_struct_type](ECPGmake_struct_type.md)
  - [ECPGdump_a_struct](ECPGdump_a_struct.md)
  - [ECPGfree_struct_member](ECPGfree_struct_member.md)
  - [find_struct_member](../f/find_struct_member.md)

## Notes and Other Information
- This structure is specifically used in the ECPG preprocessor (src/interfaces/ecpg/preproc/) for handling embedded SQL in C programs
- The linked list design allows for dynamic representation of structs with varying numbers of members
- Memory management functions like ECPGfree_struct_member handle proper cleanup of the linked list
- The structure is essential for type checking and code generation in ECPG preprocessing