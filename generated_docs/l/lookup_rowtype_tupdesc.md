# lookup_rowtype_tupdesc

## Location
src/backend/utils/cache/typcache.c: 1833 - 1849

## Overview
Public function to lookup a row type's tuple descriptor with reference counting and error reporting, ensuring proper memory management for composite types.

## Definition


## Detailed Description
This function serves as the primary public interface for retrieving tuple descriptors for composite types in PostgreSQL. It wraps the internal lookup_rowtype_tupdesc_internal() function while adding crucial memory management features:

1. **Reference Counting**: Automatically increments the reference count of the returned TupleDesc via PinTupleDesc(), ensuring the descriptor remains valid until explicitly released.

2. **Resource Management**: Logs the reference in CurrentResourceOwner to enable automatic cleanup during transaction abort or process termination.

3. **Error Handling**: Always reports errors (never returns NULL) when the requested type cannot be found, making it suitable for user-facing operations.

The function handles both named composite types and transient record types transparently. Callers are responsible for calling ReleaseTupleDesc() when finished with the tuple descriptor to properly manage memory and prevent leaks.

## Parameters / Member Variables
- : The OID of the composite type to look up
- : Type modifier for transient record types (ignored for named composite types)

## Dependencies
- Functions called/Symbols referenced:
  - [lookup_rowtype_tupdesc_internal](lookup_rowtype_tupdesc_internal.md)
  - PinTupleDesc
- Called from (representative examples):
  - [record_in](../r/record_in.md)
  - [record_out](../r/record_out.md)
  - [record_recv](../r/record_recv.md)
  - [record_send](../r/record_send.md)
  - [record_cmp](../r/record_cmp.md)
  - [record_eq](../r/record_eq.md)
  - composite_to_json
  - [make_expanded_record_from_typeid](../m/make_expanded_record_from_typeid.md)
  - [ExecuteCallStmt](../E/ExecuteCallStmt.md)
  - [ExecInitExprRec](../E/ExecInitExprRec.md)

## Notes and Other Information
- This is the standard public API for tuple descriptor lookup with proper reference counting
- Callers must call ReleaseTupleDesc() to avoid memory leaks
- Some returned tuple descriptors may not be reference-counted (guaranteed to live until process exit)
- Always throws errors on failure, making it suitable for user-facing operations
- Used extensively throughout PostgreSQL for record/composite type operations
- Part of the type cache system that provides efficient lookup of type metadata