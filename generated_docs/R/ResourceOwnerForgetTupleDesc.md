# ResourceOwnerForgetTupleDesc

## Location
[src/backend/access/common/tupdesc.c:54-66](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/tupdesc.c#L54-L66)

## Overview
A convenience wrapper function that unregisters a TupleDesc from a resource owner, removing it from automatic cleanup tracking.

## Definition


## Detailed Description
This is a static inline convenience wrapper around the generic ResourceOwnerForget function, specifically designed for TupleDesc objects. It simplifies the process of unregistering a tuple descriptor from a resource owner by automatically handling the type-specific resource descriptor (tupdesc_resowner_desc) and datum conversion. This is typically called when a TupleDesc is being manually cleaned up before the resource owner is released, ensuring that the resource owner doesn't attempt to clean it up again.

## Parameters / Member Variables
- `owner`: The ResourceOwner from which to unregister this TupleDesc
- `tupdesc`: The TupleDesc to be unregistered from the resource owner

## Dependencies
- Functions called/Symbols referenced:
  - ResourceOwnerForget
  - [PointerGetDatum](../P/PointerGetDatum.md) (implicit)
  - tupdesc_resowner_desc (resource descriptor)
- Called from (representative examples):
  - [DecrTupleDescRefCount](../D/DecrTupleDescRefCount.md)

## Notes and Other Information
- This is a static inline function, so it's only visible within the tupdesc.c compilation unit
- Part of PostgreSQL's resource owner system for automatic memory management
- Should be called as the counterpart to ResourceOwnerRememberTupleDesc when manually cleaning up a TupleDesc
- The function converts the TupleDesc pointer to a Datum using PointerGetDatum for removal from the generic resource owner system
- Prevents double-cleanup by removing the TupleDesc from automatic resource owner cleanup when it's being manually freed