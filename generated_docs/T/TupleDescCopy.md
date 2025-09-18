# TupleDescCopy

## Location
src/backend/access/common/tupdesc.c: 251 - 288

## Overview
Copies a tuple descriptor into caller-supplied memory without copying constraints and defaults, primarily used for shared memory scenarios.

## Definition


## Detailed Description
This function performs a flat copy of a tuple descriptor into pre-allocated memory provided by the caller. Unlike CreateTupleDescCopyConstr, this function explicitly does NOT copy constraints, defaults, or other metadata. It performs a direct memory copy of the header and attribute array, then clears all constraint-related fields in the destination. The function is designed for scenarios where the tuple descriptor needs to be placed in specific memory locations, such as shared memory, and where constraints are not needed.

## Parameters / Member Variables
- : Destination TupleDesc (must be pre-allocated with sufficient memory)
- : Source TupleDesc to copy from

## Dependencies
- Functions called/Symbols referenced:
  - TupleDescSize
- Called from (representative examples):
  - index_truncate_tuple
  - share_tupledesc

## Notes and Other Information
- Does NOT copy constraints, defaults, or missing values (explicitly cleared)
- Requires caller to pre-allocate memory of size TupleDescSize(src)
- Clears constraint-related attribute flags (attnotnull, atthasdef, atthasmissing, attidentity, attgenerated)
- Sets destination reference count to -1 (not ref-counted)
- Designed for shared memory usage where memory address may vary
- More efficient than CreateTupleDescCopyConstr when constraints are not needed