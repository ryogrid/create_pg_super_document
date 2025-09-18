# object_address_comparator

## Location
src/backend/catalog/dependency.c: 2443 - 2486

## Overview
A static comparison function used by qsort to order ObjectAddress items for dependency processing, with primary sorting by OID in descending order to prioritize newer objects for deletion.

## Definition


## Detailed Description
This function implements a three-level comparison algorithm for ObjectAddress structures used in PostgreSQL's dependency tracking system. The comparator is designed specifically for dependency deletion ordering, where newer objects (higher OIDs) should typically be deleted before older ones to maintain referential integrity.

The comparison follows a strict hierarchy:
1. **Primary key**: objectId in descending order (newer objects first)
2. **Secondary key**: classId in ascending order (arbitrary but consistent ordering)
3. **Tertiary key**: objectSubId as unsigned int (whole object subId=0 comes first)

The unsigned casting of objectSubId ensures that subId 0 (representing the whole object) is prioritized over specific sub-components, which is crucial for proper dependency elimination and object deletion ordering.

## Parameters / Member Variables
- : Pointer to first ObjectAddress structure to compare
- : Pointer to second ObjectAddress structure to compare

## Dependencies
- Functions called/Symbols referenced:
  - [ObjectAddress](../O/ObjectAddress.md) (struct type)
- Called from (representative examples):
  - [eliminate_duplicate_dependencies](../e/eliminate_duplicate_dependencies.md)
  - [sort_object_addresses](../s/sort_object_addresses.md)
  - [findDependentObjects](../f/findDependentObjects.md)
  - find_expr_references_context

## Notes and Other Information
- This comparator is specifically designed for qsort operations on ObjectAddress arrays
- The descending OID sort order is intentional for dependency deletion scenarios
- Unsigned comparison of objectSubId ensures subId 0 (whole object) sorts first
- Used internally by dependency tracking subsystem for maintaining proper deletion order
- Critical for eliminate_duplicate_dependencies and findDependentObjects functions