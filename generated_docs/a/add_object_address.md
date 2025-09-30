# add_object_address

## Location
[src/backend/catalog/dependency.c:2506-2532](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/dependency.c#L2506-L2532)

## Overview
A static utility function that adds a new ObjectAddress entry to an ObjectAddresses array, automatically expanding the array capacity when needed.

## Definition

```c
static void
add_object_address(Oid classId, Oid objectId, int32 subId,
				   ObjectAddresses *addrs)
```
## Detailed Description
This function provides the core mechanism for dynamically adding object references to an ObjectAddresses collection. It handles automatic array expansion using a doubling strategy when the current capacity is exceeded, ensuring efficient memory usage and amortized constant-time insertion.

When the array needs to expand, it doubles the maximum capacity (maxrefs *= 2) and reallocates the memory using repalloc. The function includes an assertion that the 'extras' array should not be present when expanding, indicating this is a simpler version that doesn't handle additional metadata.

After ensuring adequate capacity, the function creates a new ObjectAddress entry at the end of the array and populates it with the provided classId, objectId, and subId values, then increments the numrefs counter.

## Parameters / Member Variables
- : The OID of the catalog (system table) that contains the object
- : The OID of the object itself within that catalog
- : Sub-object identifier (0 for the whole object, >0 for specific parts like table columns)
- : Pointer to the ObjectAddresses structure to modify

## Dependencies
- Functions called/Symbols referenced:
  - [repalloc](../r/repalloc.md) (memory reallocation)
  - ObjectAddresses (struct type)
  - Assert (debugging macro)
- Called from (representative examples):
  - [find_expr_references_walker](../f/find_expr_references_walker.md) (extensively, 50+ call sites)
  - [process_function_rte_ref](../p/process_function_rte_ref.md)

## Notes and Other Information
- Uses doubling strategy for array growth (efficient for many insertions)
- Static function, only used within dependency.c
- Does not handle duplicate detection - callers must manage duplicates if needed
- Assert ensures 'extras' array is NULL when expanding (no metadata variant)
- Primary workhorse function for building dependency collections during expression analysis
- Memory management handled automatically through PostgreSQL's memory context system

## Simplified Source

```c
static void add_object_address(Oid classId, Oid objectId, int32 subId,
                              ObjectAddresses *addrs) {
    // Expand array if needed (double the size)
    if (addrs->numrefs >= addrs->maxrefs) {
        addrs->maxrefs *= 2;
        addrs->refs = repalloc(addrs->refs,
                              addrs->maxrefs * sizeof(ObjectAddress));
    }

    // Add new object address entry
    ObjectAddress *item = &addrs->refs[addrs->numrefs];
    item->classId = classId;
    item->objectId = objectId;
    item->objectSubId = subId;
    addrs->numrefs++;
}
```