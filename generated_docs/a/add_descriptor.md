# add_descriptor

## Location
[src/interfaces/ecpg/preproc/descriptor.c:76-98](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/preproc/descriptor.c#L76-L98)

## Overview
Adds a new SQL descriptor to the global descriptors linked list, storing its name and optional connection information for ECPG preprocessor management.

## Definition

```c
struct descriptor *new;
```
## Detailed Description
This function creates a new descriptor entry and adds it to the head of the global descriptors linked list. It performs validation to ensure the descriptor name starts with a quote character before processing. The function allocates memory for the descriptor structure and copies both the name and optional connection string.

The descriptor is added to the beginning of the list, implementing a stack-like LIFO behavior. If a connection string is provided, it's copied into allocated memory; otherwise, the connection field is set to NULL.

## Parameters / Member Variables
- : The name of the descriptor (must start with '"' to be processed)
- : Optional connection string (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [mm_alloc](../m/mm_alloc.md) (memory allocation function)
  - strcpy (string copy function)
  - [descriptor](../d/descriptor.md) (struct type)
- Called from (representative examples):
  - Grammar rules in ecpg.trailer for descriptor declarations

## Notes and Other Information
- Returns early if the name doesn't start with a quote character (\")
- Uses mm_alloc for memory management within the ECPG preprocessor
- The global 'descriptors' variable maintains the linked list of descriptors
- Memory is allocated separately for name and connection strings to ensure proper lifetime management
- Connection parameter can be NULL, in which case the connection field is also set to NULL
- Part of the ECPG preprocessor's descriptor management system for handling SQL descriptor declarations