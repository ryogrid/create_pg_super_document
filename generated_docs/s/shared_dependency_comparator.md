# shared_dependency_comparator

## Location
[src/backend/catalog/pg_shdepend.c:610-675](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_shdepend.c#L610-L675)

## Overview
A qsort comparator function used to sort ShDependObjectInfo items during shared dependency reporting, providing a deterministic ordering based on object properties and dependency types.

## Definition
```c
static int shared_dependency_comparator(const void *a, const void *b)
```

## Detailed Description
This static function serves as a comparison function for qsort when sorting arrays of ShDependObjectInfo structures. It implements a multi-level sorting hierarchy to ensure consistent ordering of dependency objects during dependency analysis and reporting. The function sorts primarily by object OID (ascending), then by catalog ID, object subId (with 0 representing whole objects coming first), and finally by dependency type. This ordering helps ensure that dependency reports are presented in a logical and reproducible manner.

## Parameters / Member Variables
- `a`: Pointer to the first ShDependObjectInfo structure to compare
- `b`: Pointer to the second ShDependObjectInfo structure to compare

## Dependencies
- Functions called/Symbols referenced:
  - ShDependObjectInfo (structure type)
- Called from (representative examples):
  - qsort function in dependency reporting context (around line 810-811 in pg_shdepend.c)

## Notes and Other Information
- The function follows standard qsort comparator semantics: returns negative for a<b, positive for a>b, and 0 for equality
- Primary sort key is objectId in ascending order to group objects by their unique identifiers
- Secondary sort on classId handles cases where identical OIDs might exist across different system catalogs
- SubId is cast to unsigned int to ensure that 0 (whole object) appears before positive subIds (object parts)
- Final sort on deptype ensures consistent ordering when the same object has multiple dependency relationships
- Used specifically in shared dependency analysis to maintain deterministic output ordering

## Simplified Source

```c
static int shared_dependency_comparator(const void *a, const void *b) {
    const ShDependObjectInfo *obja = (const ShDependObjectInfo *) a;
    const ShDependObjectInfo *objb = (const ShDependObjectInfo *) b;

    // Primary sort: object OID ascending
    if (obja->object.objectId != objb->object.objectId)
        return (obja->object.objectId < objb->object.objectId) ? -1 : 1;

    // Secondary sort: catalog ID
    if (obja->object.classId != objb->object.classId)
        return (obja->object.classId < objb->object.classId) ? -1 : 1;

    // Tertiary sort: subId as unsigned (0 comes first)
    if (obja->object.objectSubId != objb->object.objectSubId)
        return ((unsigned int)obja->object.objectSubId <
                (unsigned int)objb->object.objectSubId) ? -1 : 1;

    // Final sort: dependency type
    if (obja->deptype != objb->deptype)
        return (obja->deptype < objb->deptype) ? -1 : 1;

    return 0;  // Objects are equal
}
```