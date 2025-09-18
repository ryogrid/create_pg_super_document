# lookup_rowtype_tupdesc_copy

## Location
src/backend/utils/cache/typcache.c: 1867 - 1888

## Overview
Public function to lookup a row type's tuple descriptor and return an independent copy in the current memory context without reference counting.

## Definition


## Detailed Description
This function provides a specialized variant of tuple descriptor lookup that returns an independent copy rather than a reference-counted shared descriptor. Key characteristics include:

1. **Memory Independence**: The returned TupleDesc is allocated in CurrentMemoryContext, making it independent of the type cache's memory management.

2. **No Reference Counting**: Unlike lookup_rowtype_tupdesc(), the returned descriptor is not reference-counted, eliminating the need for ReleaseTupleDesc() calls.

3. **Full Deep Copy**: Uses CreateTupleDescCopyConstr() to create a complete copy including all constraints, defaults, and metadata.

4. **Error Handling**: Always reports errors when the type cannot be found, similar to lookup_rowtype_tupdesc().

This function is particularly useful when callers need to modify the tuple descriptor or when the descriptor needs to outlive the type cache entry. The copy semantics ensure that modifications don't affect the cached version and that the descriptor can be safely used across memory context switches.

## Parameters / Member Variables
- : The OID of the composite type to look up
- : Type modifier for transient record types (ignored for named composite types)

## Dependencies
- Functions called/Symbols referenced:
  - [lookup_rowtype_tupdesc_internal](lookup_rowtype_tupdesc_internal.md)
  - [CreateTupleDescCopyConstr](../C/CreateTupleDescCopyConstr.md)
- Called from (representative examples):
  - [ExecInitExprRec](../E/ExecInitExprRec.md)
  - [ExecMakeTableFunctionResult](../E/ExecMakeTableFunctionResult.md)
  - [get_expr_result_type](../g/get_expr_result_type.md)
  - [internal_get_result_type](../i/internal_get_result_type.md)
  - [TypeGetTupleDesc](../T/TypeGetTupleDesc.md)

## Notes and Other Information
- This is a public API function that provides copy semantics for tuple descriptors
- No need to call ReleaseTupleDesc() since the returned descriptor is not reference-counted
- The copy is allocated in CurrentMemoryContext and subject to that context's lifetime
- Suitable for scenarios requiring tuple descriptor modification or extended lifetime
- Always throws errors on failure, making it unsuitable for tentative lookups
- More expensive than reference-counted variants due to the deep copy operation
- Part of the type cache system's flexible memory management interface