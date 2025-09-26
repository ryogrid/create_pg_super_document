# find_struct

## Location
[src/interfaces/ecpg/preproc/variable.c:126-176](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/preproc/variable.c#L126-L176)

## Overview
Resolves struct/union variable references by parsing pointer and member access syntax, validating type compatibility, and delegating to find_struct_member for detailed member traversal.

## Definition

```c
static struct variable *
find_struct(char *name, char *next, char *end)
```
## Detailed Description
This function serves as the entry point for resolving complex struct and union variable references in the ECPG preprocessor. It handles the initial parsing and validation of struct access patterns, distinguishing between:

- Direct member access (struct.member)
- Pointer member access (struct->member)  
- Array element access (struct[index].member)

The function performs type checking to ensure the variable being accessed is compatible with the requested access pattern (e.g., verifying a variable is a pointer before allowing -> access). After validation, it delegates the actual member traversal to find_struct_member.

This is a critical component of ECPG's variable resolution system, enabling embedded SQL statements to reference complex C data structures.

## Parameters / Member Variables
- : The base variable name being accessed
- : Pointer to the character following the base name (either '-' for ->, '.' for direct access, or '[' for array access)
- : Pointer to the start of the member access path

## Dependencies
- Functions called/Symbols referenced:
  - [find_variable](find_variable.md) (looks up the base variable)
  - [find_struct_member](find_struct_member.md) (handles detailed member traversal)
  - mmfatal (error reporting function)
  - ECPGt_array, ECPGt_struct, ECPGt_union (ECPG type constants)
  - PARSE_ERROR (error constant)

- Called from (representative examples):
  - [find_variable](find_variable.md) (main variable resolution function)

## Notes and Other Information
- This is a static function, only accessible within the variable.c file
- Performs extensive type validation before delegating to find_struct_member
- Handles both pointer dereference (->) and direct member access (.) syntax
- Supports array indexing syntax for both simple arrays and arrays of structs
- Temporarily modifies the input string by null-terminating sections, then restores them
- Essential for ECPG's ability to handle complex C variable references in embedded SQL contexts