# pltcl_init_tuple_store

## Location
[src/pl/tcl/pltcl.c:3264-3300](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/tcl/pltcl.c#L3264-L3300)

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
  - [tuplestore_begin_heap](../t/tuplestore_begin_heap.md)
  - [TupleDescGetAttInMetadata](../T/TupleDescGetAttInMetadata.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - Assert (multiple calls for validation)
  - SFRM_Materialize_Random
- Called from (representative examples):
  - [pltcl_returnnext](pltcl_returnnext.md)

## Notes and Other Information
- This function should only be called for Set Returning Functions (SRFs), verified by Assert(rsi)
- It should only be called once per function execution, verified by Assert(!call_state->tuple_store) and Assert(!call_state->attinmeta)
- The function expects the caller to provide an appropriate result tuple descriptor via rsi->expectedDesc
- Memory context and resource owner management is critical to ensure the tuplestore persists across subtransaction boundaries
- The tuplestore is created with support for random access if the caller allows SFRM_Materialize_Random mode
- The work_mem setting is used to control memory usage for the tuplestore
- Both the tuplestore and AttInMetadata are created in the same memory context for consistency

## Simplified Source

```c
static void
pltcl_init_tuple_store(pltcl_call_state *call_state)
{
    ReturnSetInfo *rsi = call_state->rsi;
    MemoryContext oldcxt;
    ResourceOwner oldowner;

    // Validation: Must be SRF and first time initialization
    Assert(rsi);
    Assert(!call_state->tuple_store);
    Assert(!call_state->attinmeta);
    Assert(rsi->expectedDesc);

    // Set tuple descriptor from expected result
    call_state->ret_tupdesc = rsi->expectedDesc;

    // Switch to proper memory context and resource owner
    // This ensures tuplestore persists beyond subtransactions
    oldcxt = MemoryContextSwitchTo(call_state->tuple_store_cxt);
    oldowner = CurrentResourceOwner;
    CurrentResourceOwner = call_state->tuple_store_owner;

    // Create tuplestore with random access support if allowed
    call_state->tuple_store =
        tuplestore_begin_heap(rsi->allowedModes & SFRM_Materialize_Random,
                              false, work_mem);

    // Create metadata for tuple attribute handling
    call_state->attinmeta = TupleDescGetAttInMetadata(call_state->ret_tupdesc);

    // Restore previous context and owner
    CurrentResourceOwner = oldowner;
    MemoryContextSwitchTo(oldcxt);
}
```