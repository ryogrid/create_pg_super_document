# ECPGmake_struct_member

## Location
[src/interfaces/ecpg/preproc/type.c:77-95](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/preproc/type.c#L77-L95)

## Overview
Creates and appends a new struct member to a linked list of struct members for ECPG type system, managing both the member name and type information.

## Definition

```c
void
ECPGmake_struct_member(const char *name, struct ECPGtype *type, struct ECPGstruct_member **start)
```
## Detailed Description
The  function is responsible for creating a new struct member node and adding it to the end of a linked list of struct members. This function is part of the ECPG type system that handles C struct definitions during preprocessing. It allocates memory for a new struct member, copies the provided name, preserves the type pointer, and maintains the linked list structure by appending the new member to the end of the list. This ensures proper ordering of struct members as they are encountered during parsing.

## Parameters / Member Variables
- : The name of the struct member (copied into the new member structure)
- : Pointer to the ECPGtype structure representing the member's type (preserved as reference)
- : Double pointer to the beginning of the struct member linked list (modified to point to new list head if list was empty)

## Dependencies
- Functions called/Symbols referenced:
  - [mm_alloc](../m/mm_alloc.md) (memory allocation with error checking)
  - [mm_strdup](../m/mm_strdup.md) (string duplication with error checking)
  - ECPGstruct_member (struct type for linked list nodes)
  - ECPGtype (struct type for type information)
- Called from (representative examples):
  - [ECPGstruct_member_dup](ECPGstruct_member_dup.md)
  - Referenced in ECPGtype structure definition

## Notes and Other Information
- The function creates a copy of the name string but preserves the type pointer as-is
- New members are always appended to the end of the linked list to maintain declaration order
- The function handles both empty lists (where *start is NULL) and non-empty lists
- Memory management relies on the mm_alloc and mm_strdup functions for consistent error handling
- Located in  at lines 77-95