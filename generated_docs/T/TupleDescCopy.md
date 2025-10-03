# TupleDescCopy

## Location
[src/backend/access/common/tupdesc.c:251-288](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/tupdesc.c#L251-L288)

## Overview
Copies a tuple descriptor into caller-supplied memory without copying constraints and defaults, primarily used for shared memory scenarios.

## Definition

```c
void
TupleDescCopy(TupleDesc dst, TupleDesc src)
```
## Detailed Description
This function performs a flat copy of a tuple descriptor into pre-allocated memory provided by the caller. Unlike CreateTupleDescCopyConstr, this function explicitly does NOT copy constraints, defaults, or other metadata. It performs a direct memory copy of the header and attribute array, then clears all constraint-related fields in the destination. The function is designed for scenarios where the tuple descriptor needs to be placed in specific memory locations, such as shared memory, and where constraints are not needed.

## Parameters / Member Variables
- `dst`: Destination TupleDesc (must be pre-allocated with sufficient memory)
- `src`: Source TupleDesc to copy from
## Dependencies
- Functions called/Symbols referenced:
  - TupleDescSize
- Called from (representative examples):
  - [index_truncate_tuple](../i/index_truncate_tuple.md)
  - [share_tupledesc](../s/share_tupledesc.md)

## Notes and Other Information
- Does NOT copy constraints, defaults, or missing values (explicitly cleared)
- Requires caller to pre-allocate memory of size TupleDescSize(src)
- Clears constraint-related attribute flags (attnotnull, atthasdef, atthasmissing, attidentity, attgenerated)
- Sets destination reference count to -1 (not ref-counted)
- Designed for shared memory usage where memory address may vary
- More efficient than CreateTupleDescCopyConstr when constraints are not needed

## Simplified Source

```c
// Simplified version of TupleDescCopy
void TupleDescCopy(TupleDesc dst, TupleDesc src) {
    // Step 1: Copy the entire structure including header and attribute array
    memcpy(dst, src, TupleDescSize(src));

    // Step 2: Clear constraint-related fields since we don't copy constraints
    for (int i = 0; i < dst->natts; i++) {
        Form_pg_attribute att = TupleDescAttr(dst, i);

        // Clear all constraint-related flags
        att->attnotnull = false;
        att->atthasdef = false;
        att->atthasmissing = false;
        att->attidentity = '\0';
        att->attgenerated = '\0';
    }

    // Step 3: Clear constraint structure and set as non-reference-counted
    dst->constr = NULL;
    dst->tdrefcount = -1;
}
```

Key simplifications made:
- Combined the flat copy and constraint clearing into clear steps
- Simplified loop variable declaration
- Added explanatory comments for each major operation
- Focused on the core purpose: copy structure but remove constraint information
- Maintained all essential functionality while improving code clarity