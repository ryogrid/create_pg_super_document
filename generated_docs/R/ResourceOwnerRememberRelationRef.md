# ResourceOwnerRememberRelationRef

## Location
src/backend/utils/cache/relcache.c: 2142 - 2146

## Overview
A convenience wrapper function that registers a relation reference with a resource owner to track it for automatic cleanup.

## Definition


## Detailed Description
This inline function serves as a convenience wrapper around the generic ResourceOwnerRemember() function, specifically tailored for relation references. It registers a relation reference with the specified resource owner using the relation-specific resource descriptor (relref_resowner_desc).

The function is part of PostgreSQL's resource management system that ensures proper cleanup of resources when transactions abort or complete. By registering relation references with resource owners, PostgreSQL can automatically release relation references during error recovery, preventing resource leaks.

## Parameters / Member Variables
- : The ResourceOwner that should track this relation reference
- : The Relation whose reference should be remembered for cleanup

## Dependencies
- Functions called/Symbols referenced:
  - ResourceOwnerRemember
  - PointerGetDatum (implicit conversion)
  - relref_resowner_desc (resource descriptor)
- Called from (representative examples):
  - RelationIncrementReferenceCount

## Notes and Other Information
- This is a static inline function, so it gets inlined at compile time for efficiency
- Part of PostgreSQL's resource owner mechanism for automatic resource cleanup
- Paired with ResourceOwnerForgetRelationRef() for removing tracked references
- Essential for transaction safety and preventing resource leaks during error conditions
- The relref_resowner_desc provides the specific cleanup callbacks for relation references
- Used internally by the relation cache management system