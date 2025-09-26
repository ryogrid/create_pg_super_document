# _defines

## Location
[src/interfaces/ecpg/preproc/type.h:178-187](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/preproc/type.h#L178-L187)

## Overview
The `_defines` struct manages preprocessor macro definitions in PostgreSQL's ECPG preprocessor, tracking macro names, values, and command-line overrides with recursive expansion prevention.

## Definition
```c
struct _defines
{
    char       *name;        /* symbol's name */
    char       *value;       /* current value, or NULL if undefined */
    const char *cmdvalue;    /* value set on command line, or NULL */
    void       *used;        /* buffer pointer, or NULL */
    struct _defines *next;   /* list link */
};
```

## Detailed Description
This structure represents preprocessor macro definitions in the ECPG preprocessor, managing both program-defined macros and command-line definitions. It supports multiple files per compilation run by maintaining separate current and command-line values. The structure includes a mechanism to prevent recursive macro expansion through the `used` field, which tracks the buffer context during macro expansion.

## Parameters / Member Variables
- `name`: Pointer to the macro symbol's name string
- `value`: Pointer to the current macro value string, or NULL if undefined
- `cmdvalue`: Pointer to the value set via command line (-D switch), or NULL if not set
- `used`: Void pointer to buffer context during macro expansion, used to prevent recursive expansion
- `next`: Pointer to the next _defines node in the linked list

## Dependencies
- Functions called/Symbols referenced:
  - _defines (self-reference for linked list structure)
- Called from (representative examples):
  - add_preprocessor_define (in src/interfaces/ecpg/preproc/ecpg.c:94, 96)
  - main (in src/interfaces/ecpg/preproc/ecpg.c:361, 362, 363)

## Notes and Other Information
- This structure is part of the ECPG preprocessor implementation (src/interfaces/ecpg/preproc/type.h:178-187)
- Supports both program-defined macros and command-line definitions (-D switches)
- Implements recursive expansion prevention through the `used` buffer pointer mechanism
- Allows reverting to command-line definitions when processing multiple files
- The `name` and `value` fields are separately malloc'd strings, while `cmdvalue` typically points to static storage
- Used extensively by the preprocessor's main function for macro management