# BuildHardcodedDescriptor

## Location
[src/backend/utils/cache/relcache.c:4425-4454](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relcache.c#L4425-L4454)

## Overview
Creates a hardcoded tuple descriptor from predefined attribute data, serving as a workaround to access non-fixed-width catalog fields before standard catalog caches are available.

## Definition

```c
static TupleDesc
BuildHardcodedDescriptor(int natts, const FormData_pg_attribute *attrs)
```
## Detailed Description
BuildHardcodedDescriptor constructs a tuple descriptor using predefined attribute information, primarily used for accessing pg_class and pg_index catalogs during early PostgreSQL initialization phases. This function creates a "kluge" descriptor that bypasses the normal catalog cache dependency by using hardcoded attribute definitions.

The function operates in the CacheMemoryContext to ensure the descriptor persists beyond the current transaction. It creates a template tuple descriptor and manually copies attribute information from the provided array. While the resulting descriptor is not fully compliant (missing correct rowtype OID and TupleConstr), it provides sufficient functionality for field extraction during bootstrap phases.

The implementation ensures proper attribute cache offset initialization, setting the first attribute's attcacheoff to 0 and marking others as invalid (-1) for later computation.

## Parameters
- : Number of attributes in the tuple descriptor
- : Array of FormData_pg_attribute structures containing predefined attribute definitions

## Dependencies
- Functions called/Symbols referenced:
  - [CreateTemplateTupleDesc](../C/CreateTemplateTupleDesc.md)
  - ATTRIBUTE_FIXED_PART_SIZE
- Called from:
  - [GetPgClassDescriptor](../G/GetPgClassDescriptor.md)
  - [GetPgIndexDescriptor](../G/GetPgIndexDescriptor.md)

## Notes and Other Information
- The resulting tuple descriptor has limitations: incorrect tdtypeid (set to RECORDOID) and missing TupleConstr field
- Memory allocation occurs in CacheMemoryContext for persistence
- Used specifically during PostgreSQL initialization when standard catalog caches are not yet available
- Part of the relcache bootstrap mechanism that enables access to system catalogs before full initialization