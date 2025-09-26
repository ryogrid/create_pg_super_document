# pltcl_proc_ptr

## Location
[src/pl/tcl/pltcl.c:202-206](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/tcl/pltcl.c#L202-L206)

## Overview
A hash table entry structure that maps PL/Tcl function keys to their corresponding procedure descriptors, serving as the container for hash table storage.

## Definition
```c
typedef struct pltcl_proc_ptr
{
    pltcl_proc_key proc_key;    /* Hash key (must be first!) */
    pltcl_proc_desc *proc_ptr;
} pltcl_proc_ptr;
```

## Detailed Description
The `pltcl_proc_ptr` structure serves as a hash table entry that connects `pltcl_proc_key` identifiers with their corresponding `pltcl_proc_desc` descriptors. This indirection layer is crucial for PostgreSQL's hash table implementation and provides important benefits for memory management and error recovery.

The structure follows PostgreSQL's hash table conventions by placing the key as the first member, which allows the hash table implementation to efficiently access the key data. The separation between the key and the actual procedure descriptor simplifies error handling during function compilation - if compilation fails, the hash entry can be easily removed without complex cleanup of partial procedure state.

This design supports the caching strategy where compiled procedure information is retained across multiple calls to the same function, significantly improving performance for repeated function invocations.

## Parameters / Member Variables
- `proc_key`: The composite key identifying the cached procedure, containing function OID, trigger status, and user ID; must be the first field for hash table compatibility
- `proc_ptr`: Pointer to the actual procedure descriptor containing all cached compilation and execution information

## Dependencies
- Functions called/Symbols referenced:
  - [pltcl_proc_key](pltcl_proc_key.md) (at line 204) - the hash key structure
  - [pltcl_proc_desc](pltcl_proc_desc.md) (at line 205) - the procedure descriptor structure
- Called from (representative examples):
  - [_PG_init](../P/_PG_init.md) (referenced at line 456)
  - [compile_pltcl_function](../c/compile_pltcl_function.md) (referenced at line 1406)

## Notes and Other Information
- The `proc_key` field must be first to serve as the hash key in PostgreSQL's hash table implementation
- This indirection design simplifies error recovery during procedure compilation by separating key management from descriptor lifecycle
- The structure enables efficient lookup while maintaining clean separation of concerns between identification and cached data
- Used in conjunction with PostgreSQL's generic hash table facilities for managing procedure cache
- The pointer-based design allows the same procedure descriptor to be referenced by multiple keys if needed
- Located in src/pl/tcl/pltcl.c at lines 202-206