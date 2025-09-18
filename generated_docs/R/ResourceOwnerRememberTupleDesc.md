# ResourceOwnerRememberTupleDesc

## Location
[src/backend/access/common/tupdesc.c:48-53](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/tupdesc.c#L48-L53)

## Overview
A convenience wrapper function that registers a TupleDesc with a resource owner for automatic cleanup when the resource owner is released.

## Definition


## Detailed Description
This is a static inline convenience wrapper around the generic ResourceOwnerRemember function, specifically designed for TupleDesc objects. It simplifies the process of registering a tuple descriptor with a resource owner by automatically handling the type-specific resource descriptor (tupdesc_resowner_desc) and datum conversion. This ensures that the TupleDesc will be properly cleaned up when the resource owner is released, preventing memory leaks.

## Parameters / Member Variables
- : The ResourceOwner that will track this TupleDesc
- : The TupleDesc to be registered with the resource owner

## Dependencies
- Functions called/Symbols referenced:
  - ResourceOwnerRemember
  - [PointerGetDatum](../P/PointerGetDatum.md) (implicit)
  - tupdesc_resowner_desc (resource descriptor)
- Called from (representative examples):
  - [IncrTupleDescRefCount](../I/IncrTupleDescRefCount.md)

## Notes and Other Information
- This is a static inline function, so it's only visible within the tupdesc.c compilation unit
- Part of PostgreSQL's resource owner system for automatic memory management
- Should be paired with ResourceOwnerForgetTupleDesc when the TupleDesc is no longer needed before the resource owner is released
- The function converts the TupleDesc pointer to a Datum using PointerGetDatum for storage in the generic resource owner system