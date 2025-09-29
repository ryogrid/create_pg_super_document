# GetDefaultOpClass

## Location
[src/backend/commands/indexcmds.c:2278-2386](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/indexcmds.c#L2278-L2386)

## Overview
Finds the default operator class for a given data type and access method combination, handling exact matches and binary-compatible alternatives with preference rules.

## Definition

```c
Oid
GetDefaultOpClass(Oid type_id, Oid am_id)
```
## Detailed Description
This function searches through all available operator classes for a specific access method to find the appropriate default operator class for a given data type. The search algorithm implements a sophisticated preference system:

1. **Exact Type Match**: Prioritizes operator classes that exactly match the target data type
2. **Binary Compatible Match**: Falls back to operator classes for binary-compatible types
3. **Preferred Type Resolution**: Among binary-compatible matches, prefers operator classes for "preferred" types in the same type category (e.g., text over bpchar for string types)

The function performs a full scan of the pg_opclass system catalog, filtering by access method and checking only those operator classes marked as default. It includes validation to ensure there are no duplicate default operator classes for the same type, which would indicate inconsistent catalog data.

For domain types, the function automatically resolves to the base type before performing the search, ensuring that domains inherit the default operator class behavior of their underlying type.

## Parameters / Member Variables
- : OID of the data type for which to find a default operator class
- : OID of the access method (btree, hash, gist, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - [getBaseType](../g/getBaseType.md) (for domain type resolution)
  - [TypeCategory](../T/TypeCategory.md) (for type category determination)
  - [table_open](../t/table_open.md), table_close (for catalog access)
  - [systable_beginscan](../s/systable_beginscan.md), systable_getnext, systable_endscan (for catalog scanning)
  - [IsBinaryCoercible](../I/IsBinaryCoercible.md) (for type compatibility checking)
  - [IsPreferredType](../I/IsPreferredType.md) (for preferred type determination)
  - [ScanKeyInit](../S/ScanKeyInit.md) (for search key setup)
- Called from (representative examples):
  - [ResolveOpClass](../R/ResolveOpClass.md) (when no explicit operator class is specified)
  - [ComputePartitionAttrs](../C/ComputePartitionAttrs.md) (for partition key default operator classes)
  - [lookup_type_cache](../l/lookup_type_cache.md) (for type cache initialization)
  - [transformIndexConstraint](../t/transformIndexConstraint.md) (during constraint processing)

## Notes and Other Information
- Returns InvalidOid if no suitable default operator class is found
- Handles the special case where multiple binary-compatible matches exist by preferring "preferred" types
- Raises an error if multiple exact matches are found, indicating catalog inconsistency
- Automatically handles domain types by resolving to their base types
- Critical for automatic operator class selection in index creation and partitioning
- The preference system resolves ambiguity for types like varchar that are compatible with multiple operator classes

## Simplified Source

```c
Oid
GetDefaultOpClass(Oid type_id, Oid am_id)
{
    Oid result = InvalidOid;
    int nexact = 0;
    int ncompatible = 0;
    int ncompatiblepreferred = 0;
    Relation rel;
    ScanKeyData skey[1];
    SysScanDesc scan;
    HeapTuple tup;
    TYPCATEGORY tcategory;

    // Resolve domains to their base types
    type_id = getBaseType(type_id);
    tcategory = TypeCategory(type_id);

    // Scan all operator classes for this access method
    rel = table_open(OperatorClassRelationId, AccessShareLock);

    ScanKeyInit(&skey[0], Anum_pg_opclass_opcmethod,
                BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(am_id));

    scan = systable_beginscan(rel, OpclassAmNameNspIndexId, true, NULL, 1, skey);

    while (HeapTupleIsValid(tup = systable_getnext(scan)))
    {
        Form_pg_opclass opclass = (Form_pg_opclass) GETSTRUCT(tup);

        // Only consider default operator classes
        if (!opclass->opcdefault)
            continue;

        if (opclass->opcintype == type_id)
        {
            // Exact match - highest priority
            nexact++;
            result = opclass->oid;
        }
        else if (nexact == 0 && IsBinaryCoercible(type_id, opclass->opcintype))
        {
            // Binary compatible match
            if (IsPreferredType(tcategory, opclass->opcintype))
            {
                // Preferred type - second highest priority
                ncompatiblepreferred++;
                result = opclass->oid;
            }
            else if (ncompatiblepreferred == 0)
            {
                // Compatible but not preferred - lowest priority
                ncompatible++;
                result = opclass->oid;
            }
        }
    }

    systable_endscan(scan);
    table_close(rel, AccessShareLock);

    // Validate result - should not have multiple exact matches
    if (nexact > 1)
        ereport(ERROR, "Multiple default operator classes for data type");

    // Return result if we found a unique match at any priority level
    if (nexact == 1 ||
        ncompatiblepreferred == 1 ||
        (ncompatiblepreferred == 0 && ncompatible == 1))
        return result;

    return InvalidOid;
}
```