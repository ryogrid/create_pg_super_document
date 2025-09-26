# pltcl_init_tuple_store

## Location
src/pl/tcl/pltcl.c: 3264 - 3300

## Overview
Initializes the result tuplestore for a Set Returning Function (SRF), setting up the necessary data structures to collect and store multiple result tuples.

## Definition
```c
static void
pltcl_init_tuple_store(pltcl_call_state *call_state)
```

## Detailed Description
This function initializes the tuplestore infrastructure required for PL/Tcl Set Returning Functions (SRFs). It creates a new tuplestore in the appropriate memory context and resource owner, ensuring that the tuplestore persists beyond subtransaction boundaries such as those created by exception blocks. The function also sets up the AttInMetadata structure needed for converting string representations to typed PostgreSQL datums when storing tuples.

The function carefully manages memory contexts and resource ownership to ensure that the tuplestore is created in the correct scope. It switches to the tuple_store_cxt memory context and tuple_store_owner resource owner before creating the tuplestore, then restores the previous context and owner. This ensures proper cleanup semantics and prevents resource leaks.

## Parameters / Member Variables
- `call_state`: Pointer to the pltcl_call_state structure containing function execution state, including memory contexts, return type descriptor, and tuplestore references

## Dependencies
- Functions called/Symbols referenced:
  - tuplestore_begin_heap
  - TupleDescGetAttInMetadata
  - MemoryContextSwitchTo
  - Assert (multiple calls for validation)
  - SFRM_Materialize_Random
- Called from (representative examples):
  - pltcl_returnnext

## Notes and Other Information
- This function should only be called for Set Returning Functions (SRFs), verified by Assert(rsi)
- It should only be called once per function execution, verified by Assert(!call_state->tuple_store) and Assert(!call_state->attinmeta)
- The function expects the caller to provide an appropriate result tuple descriptor via rsi->expectedDesc
- Memory context and resource owner management is critical to ensure the tuplestore persists across subtransaction boundaries
- The tuplestore is created with support for random access if the caller allows SFRM_Materialize_Random mode
- The work_mem setting is used to control memory usage for the tuplestore
- Both the tuplestore and AttInMetadata are created in the same memory context for consistency