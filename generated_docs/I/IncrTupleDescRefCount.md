# IncrTupleDescRefCount

## Location
src/backend/access/common/tupdesc.c: 388 - 405

## Overview
Increments the reference count of a tuple descriptor and registers it with the current resource owner for automatic cleanup.

## Definition


## Detailed Description
This function safely increments the reference count of a tuple descriptor that is being managed through PostgreSQL's reference counting system. It ensures the resource owner has sufficient capacity, increments the reference count, and registers the tuple descriptor with the current resource owner for automatic cleanup when the resource owner is destroyed. The function includes validation that the tuple descriptor is already being reference counted (tdrefcount >= 0).

## Parameters / Member Variables
- : The TupleDesc whose reference count should be incremented (must already be ref-counted)

## Dependencies
- Functions called/Symbols referenced:
  - ResourceOwnerEnlarge
  - [ResourceOwnerRememberTupleDesc](../R/ResourceOwnerRememberTupleDesc.md)
- Called from (representative examples):
  - [ExecEvalConvertRowtype](../E/ExecEvalConvertRowtype.md)
  - [cache_record_field_properties](../c/cache_record_field_properties.md)
  - PinTupleDesc (macro)

## Notes and Other Information
- Only applies to tuple descriptors already being reference counted (tdrefcount >= 0)
- Validates reference count is non-negative before incrementing
- Integrates with PostgreSQL's resource management system for automatic cleanup
- Should not be used on non-refcounted tuple descriptors (use PinTupleDesc macro instead)
- Ensures resource owner capacity is adequate before registration
- Essential for preventing memory leaks and ensuring proper cleanup in error scenarios
- Part of PostgreSQL's broader resource ownership and reference counting framework