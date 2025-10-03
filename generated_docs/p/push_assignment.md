# push_assignment

## Location
[src/interfaces/ecpg/preproc/descriptor.c:21-32](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/preproc/descriptor.c#L21-L32)

## Overview
Adds a new assignment entry to the global assignments linked list, storing a variable name and its corresponding descriptor type value.

## Definition

```c
void
push_assignment(char *var, enum ECPGdtype value)
```
## Detailed Description
This function creates a new assignment structure and adds it to the head of the global assignments linked list. It's part of the ECPG (Embedded SQL in C) preprocessor's descriptor handling mechanism. The function allocates memory for both the assignment structure and a copy of the variable name string, then links the new assignment to the existing chain.

The assignments are stored as a linked list where each new assignment becomes the head, implementing a stack-like LIFO (Last In, First Out) behavior.

## Parameters / Member Variables
- `*var`: The variable name to be assigned (copied into newly allocated memory)
- `value`: The ECPGdtype enum value representing the descriptor data type
## Dependencies
- Functions called/Symbols referenced:
  - [mm_alloc](../m/mm_alloc.md) (memory allocation function)
  - strcpy (string copy function)
  - [assignment](../a/assignment.md) (struct type)
  - ECPGdtype (enum type)
- Called from (representative examples):
  - Grammar rules in ecpg.trailer for descriptor assignments
  - ECPGGetDescItem grammar productions

## Notes and Other Information
- Uses mm_alloc instead of standard malloc for memory management within the ECPG preprocessor
- The global 'assignments' variable maintains the linked list of assignments
- Memory for the variable name is allocated separately and the string is copied to avoid dependencies on the original string's lifetime
- Part of the ECPG preprocessor's mechanism for handling SQL descriptor assignments in embedded SQL code