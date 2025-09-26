# add_typename

## Location
[src/tools/pg_bsd_indent/lexi.c:687-720](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/tools/pg_bsd_indent/lexi.c#L687-L720)

## Overview
Adds a user-defined type name to the sorted typenames array, expanding the array dynamically and maintaining sorted order while preventing duplicates.

## Definition
void add_typename(const char *key)

## Detailed Description
This function manages the dynamic typenames array by adding new type names while maintaining alphabetical order. It first checks if the array needs to be expanded, doubling its size when necessary. The function optimizes for sorted input by checking if the new key can be appended directly. For unsorted input, it performs a linear search to find the correct insertion position and uses memmove to shift existing elements. Duplicate entries are detected and ignored. The function creates a copy of the input string using strdup to ensure the typename persists beyond the callers scope.

## Parameters / Member Variables
- key: The type name string to be added to the typenames array

## Dependencies
- Functions called/Symbols referenced:
  - realloc (expands the typenames array when needed)
  - strcmp (compares strings for sorting and duplicate detection)  
  - strdup (creates a persistent copy of the key string)
  - memmove (shifts array elements for insertion)
  - [err](../e/err.md) (error reporting and program termination)
- Called from (representative examples):
  - [set_option](../s/set_option.md) (at src/tools/pg_bsd_indent/args.c:296)
  - [add_typedefs_from_file](add_typedefs_from_file.md) (at src/tools/pg_bsd_indent/args.c:347)

## Notes and Other Information
- Maintains the typenames array in sorted alphabetical order
- Doubles array capacity when expansion is needed
- Prevents duplicate entries by checking for existing keys
- Optimizes for sorted input by checking append possibility first
- Uses strdup to create persistent copies of type name strings
- Program terminates with error if memory allocation fails
- Essential for supporting custom type recognition in code formatting
- Works with both command-line specified types and types loaded from files