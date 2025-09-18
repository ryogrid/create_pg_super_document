# ArrayMetaState

## Location
src/include/utils/array.h: 236 - 246

## Overview
ArrayMetaState is a cache structure that stores type metadata needed for efficient array manipulation operations, avoiding repeated lookups of type information during array processing.

## Definition
```c
typedef struct ArrayMetaState
{
    Oid         element_type;
    int16       typlen;
    bool        typbyval;
    char        typalign;
    char        typdelim;
    Oid         typioparam;
    Oid         typiofunc;
    FmgrInfo    proc;
} ArrayMetaState;
```

## Detailed Description
ArrayMetaState serves as a performance optimization by caching frequently-needed type information for array operations. Rather than looking up type characteristics from system catalogs repeatedly, this structure stores the metadata after the first lookup and reuses it for subsequent operations. It's commonly stored in function call contexts (fcinfo->flinfo->fn_extra) to persist across multiple calls to the same array function. The structure contains all the essential information needed for array element handling, including storage characteristics, I/O functions, and function manager information for efficient operation dispatch.

## Parameters / Member Variables
- `element_type`: OID of the array element data type
- `typlen`: Length of the element type (-1 for variable-length types, positive for fixed-length)
- `typbyval`: Whether elements are passed by value (true) or by reference (false)
- `typalign`: Alignment requirement for the element type ('c', 's', 'i', or 'd')
- `typdelim`: Delimiter character used in array text representation (typically ',')
- `typioparam`: OID parameter for the I/O functions (usually the element type OID)
- `typiofunc`: OID of the input/output function for the element type
- `proc`: Cached function manager info for efficient function calls

## Dependencies
- Functions called/Symbols referenced:
  - FmgrInfo structure for function call management
  - Oid type for object identifiers
  - Type system catalogs for metadata lookup
- Called from (representative examples):
  - fetch_array_arg_replace_nulls() - array argument processing
  - array_in() / array_out() - array input/output functions
  - array_recv() / array_send() - array binary I/O functions
  - array_append() / array_prepend() - array modification functions
  - array_position_common() - array search operations

## Notes and Other Information
- Typically allocated in function memory context (fn_mcxt) for persistence across calls
- The element_type field is often used as a validity check (InvalidOid indicates uninitialized state)
- Provides significant performance improvements for functions that process arrays repeatedly
- Used extensively in array I/O, modification, and search operations
- The proc field contains cached function call information to avoid function lookup overhead
- Essential for efficient array text parsing and formatting operations
- Commonly initialized on first use and cached for subsequent function calls