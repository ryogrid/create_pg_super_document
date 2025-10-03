# remove_typedefs

## Location
[src/interfaces/ecpg/preproc/variable.c:260-288](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/preproc/variable.c#L260-L288)

## Overview
Removes all typedef definitions from the global types list that were defined at or deeper than the specified brace level, performing proper memory cleanup.

## Definition

```c
void
remove_typedefs(int brace_level)
```
## Detailed Description
This function implements scope-based cleanup for typedef definitions in the ECPG preprocessor. It traverses the global linked list of typedefs and removes all entries that were defined at a brace level greater than or equal to the specified threshold. This is crucial for maintaining proper scoping semantics when exiting code blocks.

The function performs comprehensive memory management by:
- Unlinking typedef entries from the global list
- Freeing struct/union member lists for complex types
- Deallocating type descriptors, names, and the typedef structure itself
- Maintaining list integrity during traversal and deletion

This cleanup mechanism ensures that typedefs defined in inner scopes (higher brace levels) are properly removed when those scopes are exited, preventing memory leaks and maintaining correct symbol visibility.

## Parameters / Member Variables
- `brace_level`: The minimum brace level threshold; typedefs at this level or deeper will be removed
## Dependencies
- Functions called/Symbols referenced:
  - free (C library function for memory deallocation)
  - types (global typedef list)
  - [typedefs](../t/typedefs.md) (typedef structure type)
  - ECPGt_struct, ECPGt_union (ECPG type enumeration constants)

- Called from (representative examples):
  - (No direct callers found in the current analysis, likely called during scope exit processing)

## Notes and Other Information
- This is a public function, accessible from other ECPG modules
- Uses careful pointer manipulation to maintain list integrity during deletion
- Handles both simple and complex (struct/union) typedef cleanup
- Essential for proper memory management and scope semantics in ECPG
- The brace_level parameter typically corresponds to C block nesting depth
- Prevents typedef pollution across scope boundaries in embedded SQL processing