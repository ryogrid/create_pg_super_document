# typedefs

## Location
src/interfaces/ecpg/preproc/type.h: 157 - 177

## Overview
The `typedefs` struct represents type definitions in PostgreSQL's ECPG preprocessor, managing user-defined types and their associated metadata including structure members and scope information.

## Definition
```c
struct typedefs
{
    char               *name;
    struct this_type   *type;
    struct ECPGstruct_member *struct_member_list;
    int                 brace_level;
    struct typedefs    *next;
};
```

## Detailed Description
This structure is used by the ECPG preprocessor to maintain information about type definitions (typedefs) encountered during the parsing of embedded SQL in C programs. It tracks the typedef name, its underlying type information, associated structure members, and the scope level where it was defined. The structure forms a linked list to manage multiple typedef declarations.

## Parameters / Member Variables
- `name`: Pointer to the typedef name string
- `type`: Pointer to the underlying type information structure
- `struct_member_list`: Pointer to the list of structure members if this typedef represents a struct
- `brace_level`: Integer representing the nesting level/scope where this typedef was declared
- `next`: Pointer to the next typedefs node in the linked list

## Dependencies
- Functions called/Symbols referenced:
  - this_type (struct used for type information)
  - ECPGstruct_member (struct used for structure member lists)
  - typedefs (self-reference for linked list structure)
- Called from (representative examples):
  - main (in src/interfaces/ecpg/preproc/ecpg.c:364, 429)
  - remove_typedefs (in src/interfaces/ecpg/preproc/variable.c:262)
  - check_indicator (in src/interfaces/ecpg/preproc/variable.c:497)
  - get_typedef (in src/interfaces/ecpg/preproc/variable.c:500)

## Notes and Other Information
- This structure is part of the ECPG preprocessor implementation (src/interfaces/ecpg/preproc/type.h:157-164)
- Supports scope-aware typedef management through the brace_level field
- Used extensively in variable processing and type checking operations
- Implements a linked list architecture for managing multiple typedef declarations
- Integrates with the ECPG struct member system for complex type definitions