# removeObjectDependency

## Location
src/bin/pg_dump/common.c: 832 - 851

## Overview
Removes dependency links from a DumpableObject, eliminating references to a specific object ID from the dependency list.

## Definition


## Detailed Description
This function removes all instances of a specific dependency ID from a DumpableObject's dependency array. It uses an efficient in-place compaction algorithm that iterates through the dependencies array once, copying non-matching entries to the front of the array while skipping entries that match the target ID. If multiple instances of the same dependency exist, all are removed in a single pass.

The function is particularly important in pg_dump's dependency resolution and loop-breaking logic, where circular dependencies need to be temporarily broken to establish a valid dump order. It's commonly used in various "repair" functions that handle dependency loops and boundary conditions.

## Parameters / Member Variables
- : Pointer to the DumpableObject from which the dependency will be removed
- : The DumpId of the dependency to remove from the object's dependency list

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject (structure type for dumpable objects)
  - DumpId (type for dump object identifiers)
- Called from (representative examples):
  - repairTypeFuncLoop (in src/bin/pg_dump/pg_dump_sort.c:932)
  - repairViewRuleLoop (in src/bin/pg_dump/pg_dump_sort.c:962)
  - repairViewRuleMultiLoop (in src/bin/pg_dump/pg_dump_sort.c:985)
  - repairDependencyLoop (in src/bin/pg_dump/pg_dump_sort.c:1418,1443,1445,1463,1465)
  - Multiple other repair functions for handling dependency loops

## Notes and Other Information
- The function removes ALL occurrences of the specified refId, not just the first one found
- Uses an efficient O(n) in-place compaction algorithm that preserves the order of remaining dependencies
- Does not deallocate memory - the dependencies array size remains the same but nDeps is updated to reflect the new count
- Primarily used in pg_dump_sort.c for breaking dependency loops and resolving circular references
- The function handles the case where the refId doesn't exist in the dependencies array gracefully (no-op)
- Critical for pg_dump's ability to handle complex object relationships and ensure proper dump ordering