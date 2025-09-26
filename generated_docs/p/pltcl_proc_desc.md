# pltcl_proc_desc

## Location
src/pl/tcl/pltcl.c: 140 - 162

## Overview
A comprehensive structure that caches information about loaded PL/Tcl procedures, managing their metadata, type information, memory context, and execution state.

## Definition
```c
typedef struct pltcl_proc_desc
{
    char           *user_proname;       /* user's name (from pg_proc.proname) */
    char           *internal_proname;   /* Tcl name (based on function OID) */
    MemoryContext   fn_cxt;             /* memory context for this procedure */
    unsigned long   fn_refcount;       /* number of active references */
    TransactionId   fn_xmin;            /* xmin of pg_proc row */
    ItemPointerData fn_tid;             /* TID of pg_proc row */
    bool            fn_readonly;        /* is function readonly? */
    bool            lanpltrusted;       /* is it pltcl (vs. pltclu)? */
    pltcl_interp_desc *interp_desc;     /* interpreter to use */
    Oid             result_typid;       /* OID of fn's result type */
    FmgrInfo        result_in_func;     /* input function for fn's result type */
    Oid             result_typioparam;  /* param to pass to same */
    bool            fn_retisset;        /* true if function returns a set */
    bool            fn_retistuple;      /* true if function returns composite */
    bool            fn_retisdomain;     /* true if function returns domain */
    void           *domain_info;        /* opaque cache for domain checks */
    int             nargs;              /* number of arguments */
    /* these arrays have nargs entries: */
    FmgrInfo       *arg_out_func;       /* output fns for arg types */
    bool           *arg_is_rowtype;     /* is each arg composite? */
} pltcl_proc_desc;
```

## Detailed Description
The `pltcl_proc_desc` structure serves as a comprehensive cache for PL/Tcl procedure information, storing both PostgreSQL metadata and Tcl-specific execution details. This structure is designed for efficient procedure execution by pre-computing and caching type conversion functions, parameter information, and execution context.

The structure employs reference counting (`fn_refcount`) to manage memory lifecycle. All subsidiary data is stored in the designated memory context (`fn_cxt`) and can be reclaimed by deleting that context when the reference count reaches zero. The structure handles both trusted (pltcl) and untrusted (pltclu) procedure variants.

Key design aspects:
- **Memory Management**: Uses dedicated memory context for clean resource management
- **Type Safety**: Caches input/output functions for efficient data conversion
- **Version Control**: Tracks transaction ID and tuple ID to detect pg_proc changes
- **Security**: Distinguishes between trusted and untrusted execution environments

## Parameters / Member Variables
- `user_proname`: Original function name as specified by the user in pg_proc.proname
- `internal_proname`: Internal Tcl procedure name, typically based on the function's OID for uniqueness
- `fn_cxt`: Memory context containing all data for this procedure descriptor, enabling bulk cleanup
- `fn_refcount`: Reference counter tracking active calls to prevent premature cleanup
- `fn_xmin`: Transaction ID from the pg_proc row, used for cache invalidation detection
- `fn_tid`: Tuple identifier of the pg_proc row, used for precise cache invalidation
- `fn_readonly`: Boolean flag indicating whether the function is read-only
- `lanpltrusted`: Boolean distinguishing trusted (pltcl) from untrusted (pltclu) procedures
- `interp_desc`: Pointer to the Tcl interpreter descriptor that will execute this procedure
- `result_typid`: OID of the function's return type
- `result_in_func`: Cached input function for converting Tcl results to PostgreSQL format
- `result_typioparam`: Parameter to pass to the result input function
- `fn_retisset`: Boolean indicating if the function returns a set of values
- `fn_retistuple`: Boolean indicating if the function returns a composite type
- `fn_retisdomain`: Boolean indicating if the function returns a domain type
- `domain_info`: Opaque cache for domain constraint checking information
- `nargs`: Number of function arguments
- `arg_out_func`: Array of output functions for converting PostgreSQL arguments to Tcl format
- `arg_is_rowtype`: Array of booleans indicating which arguments are composite types

## Dependencies
- Functions called/Symbols referenced:
  - pltcl_interp_desc (at line 150)
  - MemoryContext (PostgreSQL memory management)
  - TransactionId (PostgreSQL transaction system)
  - ItemPointerData (PostgreSQL tuple identifier)
  - FmgrInfo (PostgreSQL function manager)
  - Oid (PostgreSQL object identifier)
- Called from (representative examples):
  - pltcl_proc_ptr (referenced at line 205)
  - pltcl_call_state (referenced at line 221)
  - pltcl_func_handler (referenced at line 801)
  - pltcl_trigger_handler (referenced at line 1059)
  - compile_pltcl_function (referenced at lines 1408, 1409, 1502)

## Notes and Other Information
- Data in this struct is shared across all active calls; only `fn_refcount` should be modified by call instances
- Memory cleanup occurs only when `fn_refcount` reaches zero, not when pg_proc rows are deleted
- Tcl's internal procedure definition cleanup is handled separately by Tcl's memory management
- The structure supports efficient cache invalidation through `fn_xmin` and `fn_tid` tracking
- Located in src/pl/tcl/pltcl.c at lines 140-162