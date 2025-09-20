# ECPGdump_a_struct

## Location
[src/interfaces/ecpg/preproc/type.c:581-640](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/preproc/type.c#L581-L640)

## Overview
ECPGdump_a_struct is a static function that recursively processes struct types and generates ECPG runtime code for each member of the structure. It handles both the main struct and optional indicator struct, managing proper member access syntax and offset calculations.

## Definition

```c
static void
ECPGdump_a_struct(FILE *o, const char *name, const char *ind_name, char *arrsize, struct ECPGtype *type, struct ECPGtype *ind_type, const char *prefix, const char *ind_prefix)
```
## Detailed Description
This function penetrates a struct definition and recursively dumps the contents of each member by calling ECPGdump_a_type for each struct member. It determines the appropriate member access syntax (dot notation for value access, arrow notation for pointer access) based on array size. The function also handles indicator structs which are used for NULL value detection in PostgreSQL, ensuring proper alignment between main struct members and their corresponding indicator members. It performs validation to ensure indicator structs have the correct number of members.

## Parameters / Member Variables
- : Output FILE pointer where the generated code will be written
- : Name of the struct variable being processed
- : Name of the indicator struct variable (for NULL handling)
- : String representing array dimensions, determines access method (dot vs arrow)
- : ECPGtype pointer containing the main struct definition and member list
- : ECPGtype pointer for the indicator struct, or &ecpg_no_indicator if none
- : Current prefix string for nested member access
- : Current prefix string for indicator struct member access

## Dependencies
- Functions called/Symbols referenced:
  - [mm_alloc](../m/mm_alloc.md) (memory allocation for prefix buffers)
  - [ECPGdump_a_type](ECPGdump_a_type.md) (recursive call to dump individual struct members)
  - mmerror (error reporting for struct member mismatch)
  - ECPGstruct_member (struct member linked list traversal)
  - ecpg_no_indicator (special indicator for no indicator struct)
- Called from (representative examples):
  - [ECPGdump_a_type](ECPGdump_a_type.md) (when processing struct types)

## Notes and Other Information
- Uses dot notation (.) when arrsize == 1 (value access) and arrow notation (->) otherwise (pointer access)
- Maintains parallel iteration through main struct and indicator struct members
- Provides comprehensive error checking for indicator struct member count mismatches
- The struct_no_indicator is used as a placeholder when no indicator is needed for a member
- Memory allocated for prefix buffers (pbuf, ind_pbuf) is properly freed at function end
- Passes struct_sizeof information to child calls for proper offset calculations in nested structures
- Warning messages are generated when indicator struct has too few or too many members compared to main struct