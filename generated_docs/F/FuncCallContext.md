# FuncCallContext

## Location
[src/include/funcapi.h:57-114](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/funcapi.h#L57-L114)

## Overview
A support structure that maintains state and context information across multiple calls to Set Returning Functions (SRFs), enabling efficient implementation of functions that return multiple rows.

## Definition


## Detailed Description
FuncCallContext is the core structure for implementing Set Returning Functions (SRFs) in PostgreSQL. It maintains state between multiple invocations of the same function, allowing functions to return one row at a time while preserving context across calls. This structure is automatically managed by the SRF infrastructure through macros like SRF_FIRSTCALL_INIT(), SRF_RETURN_NEXT(), and SRF_RETURN_DONE().

The structure provides both automatic state management (call counters, memory context) and optional fields for user-defined state, tuple construction metadata, and call limits. It enables efficient implementation of functions that process large datasets by avoiding the need to materialize all results in memory at once.

## Parameters / Member Variables
- : Automatically incremented counter tracking how many times the function has been called (initialized to 0)
- : Optional field to specify maximum number of expected calls; used for early termination checks
- : Generic void pointer for storing user-defined context data that persists across function calls
- : Pointer to AttInMetadata structure for efficient tuple construction from C strings using BuildTupleFromCStrings()
- : Memory context for allocations that must persist across multiple function calls; automatically managed by SRF infrastructure
- : TupleDesc for constructing tuples using heap_form_tuple(); should be blessed with BlessTupleDesc()

## Dependencies
- Functions called/Symbols referenced:
  - [AttInMetadata](../A/AttInMetadata.md) (struct type)
  - [MemoryContext](../M/MemoryContext.md) (type)
  - [TupleDesc](../T/TupleDesc.md) (type)
- Called from (representative examples):
  - [init_MultiFuncCall](../i/init_MultiFuncCall.md)
  - [per_MultiFuncCall](../p/per_MultiFuncCall.md)
  - [end_MultiFuncCall](../e/end_MultiFuncCall.md)
  - generate_series functions
  - [pg_lock_status](../p/pg_lock_status.md)
  - [tsvector_unnest](../t/tsvector_unnest.md)
  - [array_unnest](../a/array_unnest.md) functions
  - [regexp_matches](../r/regexp_matches.md)
  - [jsonb_object_keys](../j/jsonb_object_keys.md)

## Notes and Other Information
- Central to PostgreSQL's Set Returning Function infrastructure
- Automatically managed by SRF macros: SRF_FIRSTCALL_INIT(), SRF_RETURN_NEXT(), SRF_RETURN_DONE()
- Enables memory-efficient processing of large result sets by returning one row at a time
- Used extensively throughout PostgreSQL core functions and extension development
- The multi_call_memory_ctx is crucial for avoiding memory leaks in long-running SRFs
- Choice between attinmeta and tuple_desc depends on whether using BuildTupleFromCStrings() or heap_form_tuple()
- Essential for implementing table functions, generators, and other multi-row returning functionality