# find_struct_member

## Location
[src/interfaces/ecpg/preproc/variable.c:25-125](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/preproc/variable.c#L25-L125)

## Overview
Recursively traverses a struct member hierarchy to find and return a specific member variable based on a dot-notation path, handling nested structs, unions, and arrays.

## Definition

```c
static struct variable *
find_struct_member(char *name, char *str, struct ECPGstruct_member *members, int brace_level)
```
## Detailed Description
This function is part of the ECPG (Embedded SQL in C) preprocessor's variable resolution system. It parses a string containing a member access path (using dot notation like "member.submember[index]") and traverses the corresponding struct member hierarchy to locate the target member. The function handles complex nested structures including:

- Simple struct/union member access with dot notation
- Array element access with bracket notation
- Nested combinations of structs, unions, and arrays
- Recursive traversal for deeply nested structures

When a member is found, it creates and returns a new variable object with the appropriate type information. The function performs syntax validation and reports errors for malformed variable references.

## Parameters / Member Variables
- : The original variable name being resolved (used for error reporting)
- : The member access string to parse (e.g., ".member[0].submember")
- : Linked list of struct members to search through
- : Current nesting level for brace counting

## Dependencies
- Functions called/Symbols referenced:
  - strpbrk (C library function for string parsing)
  - strcmp (C library function for string comparison)
  - new_variable (creates new variable objects)
  - ECPGmake_array_type (creates array type descriptors)
  - ECPGmake_simple_type (creates simple type descriptors)
  - ECPGmake_struct_type (creates struct/union type descriptors)
  - mmfatal (error reporting function)
  - ECPGstruct_member (struct member type)
  - ECPGt_array, ECPGt_struct, ECPGt_union (ECPG type constants)
  - PARSE_ERROR (error constant)

- Called from (representative examples):
  - find_struct (parent function for struct variable resolution)

## Notes and Other Information
- This is a static function, only accessible within the variable.c file
- Uses recursive calls to handle nested member access
- Performs careful string parsing with temporary null termination
- Handles bracket counting to properly skip array index expressions
- Returns NULL if the specified member path cannot be resolved
- Critical for ECPG's ability to handle complex C struct variables in embedded SQL