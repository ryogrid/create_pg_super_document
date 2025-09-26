# pltcl_call_state

## Location
[src/pl/tcl/pltcl.c:212-235](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/tcl/pltcl.c#L212-L235)

## Overview
A per-call state structure that maintains all necessary context and data for a single PL/Tcl function or trigger invocation, including parameter information, return value handling, and tuple store management for set-returning functions.

## Definition
```c
typedef struct pltcl_call_state
{
    /* Call info struct, or NULL in a trigger */
    FunctionCallInfo fcinfo;

    /* Trigger data, if we're in a normal (not event) trigger; else NULL */
    TriggerData *trigdata;

    /* Function we're executing (NULL if not yet identified) */
    pltcl_proc_desc *prodesc;

    /*
     * Information for SRFs and functions returning composite types.
     * ret_tupdesc and attinmeta are set up if either fn_retistuple or
     * fn_retisset, since even a scalar-returning SRF needs a tuplestore.
     */
    TupleDesc	ret_tupdesc;	/* return rowtype, if retistuple or retisset */
    AttInMetadata *attinmeta;	/* metadata for building tuples of that type */

    ReturnSetInfo *rsi;			/* passed-in ReturnSetInfo, if any */
    Tuplestorestate *tuple_store;	/* SRFs accumulate result here */
    MemoryContext tuple_store_cxt;	/* context and resowner for tuplestore */
    ResourceOwner tuple_store_owner;
} pltcl_call_state;
```

## Detailed Description
The `pltcl_call_state` structure serves as the central state container for PL/Tcl function execution. It is designed to handle multiple execution contexts including regular functions, triggers, event triggers, and set-returning functions (SRFs). The structure maintains a clear separation between different types of calls through mutually exclusive pointer fields and provides comprehensive support for complex return types including composite types and result sets.

The structure is instantiated on the stack in the main handler function (`pltcl_handler`) and passed by reference to specialized handler functions. A global pointer `pltcl_current_call_state` is maintained to provide access to the current call context from Tcl command implementations.

For set-returning functions, the structure manages a tuplestore along with its associated memory context and resource owner, ensuring proper memory management and cleanup. The tuple metadata (`attinmeta`) is prepared to facilitate efficient tuple construction from Tcl results.

## Parameters / Member Variables
- `fcinfo`: Standard PostgreSQL function call information structure containing arguments, return type info, and execution context. NULL when executing in trigger context.
- `trigdata`: Pointer to trigger-specific data including the triggering tuple, relation information, and trigger event details. NULL for regular function calls.
- `prodesc`: Reference to the cached procedure descriptor containing Tcl function metadata, interpreter assignment, and execution parameters. Initially NULL until function is identified.
- `ret_tupdesc`: Tuple descriptor for the return type, populated for functions returning composite types or set-returning functions to describe the structure of returned rows.
- `attinmeta`: Metadata structure used for efficient conversion of attribute values when constructing tuples for composite or set-returning functions.
- `rsi`: Return Set Info structure passed by the PostgreSQL executor for set-returning functions, containing result collection interface and execution mode information.
- `tuple_store`: Tuplestore instance where set-returning functions accumulate their result tuples before returning them to the executor.
- `tuple_store_cxt`: Memory context specifically allocated for the tuplestore to ensure proper memory lifecycle management.
- `tuple_store_owner`: Resource owner for the tuplestore to ensure proper cleanup in case of errors or transaction abort.

## Dependencies
- Functions called/Symbols referenced:
  - [FunctionCallInfo](../F/FunctionCallInfo.md)
  - [TriggerData](../T/TriggerData.md)  
  - [pltcl_proc_desc](pltcl_proc_desc.md)
  - [AttInMetadata](../A/AttInMetadata.md)
  - [ReturnSetInfo](../R/ReturnSetInfo.md)
  - [Tuplestorestate](../T/Tuplestorestate.md)
  - [ResourceOwner](../R/ResourceOwner.md)
- Called from (representative examples):
  - [pltcl_handler](pltcl_handler.md)
  - [pltcl_func_handler](pltcl_func_handler.md)
  - [pltcl_trigger_handler](pltcl_trigger_handler.md)
  - [pltcl_event_trigger_handler](pltcl_event_trigger_handler.md)
  - [pltcl_init_tuple_store](pltcl_init_tuple_store.md)

## Notes and Other Information
The structure is designed to be initialized with memset to zeros, ensuring all pointer fields start as NULL. The global variable `pltcl_current_call_state` provides access to the current call state for Tcl command implementations that need to access execution context.

The structure supports three distinct execution modes: regular function calls (fcinfo non-NULL), trigger calls (trigdata non-NULL), and event trigger calls (both fcinfo and trigdata NULL). The tuple store infrastructure is only initialized for set-returning functions or functions returning composite types.

Memory management is carefully handled through the associated memory context and resource owner, ensuring proper cleanup even in error conditions. The procedure descriptor reference counting mechanism ensures that function metadata remains valid throughout the call execution.