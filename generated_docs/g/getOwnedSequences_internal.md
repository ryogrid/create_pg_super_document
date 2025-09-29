# getOwnedSequences_internal

## Location
[src/backend/catalog/pg_depend.c:878-936](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_depend.c#L878-L936)

## Overview
Collects a list of OIDs for all sequences owned by a specified table and optionally a specific column, with optional filtering by dependency type.

## Definition

```c
static List *
getOwnedSequences_internal(Oid relid, AttrNumber attnum, char deptype)
```
## Detailed Description
The `getOwnedSequences_internal` function is a static utility that searches the `pg_depend` system catalog to find sequences that have ownership dependencies on a given table or specific column within that table. It serves as the core implementation for higher-level functions that need to identify owned sequences.

The function performs a systematic scan of the `pg_depend` table using the DependReferenceIndexId index for efficient lookups. It searches for dependency records where:
- The `refclassid` is RelationRelationId and `refobjid` matches the specified table
- If `attnum` is provided, `refobjsubid` must match the specified column number
- The `classid` is RelationRelationId (dependency originates from a relation)
- The `objsubid` is 0 (dependency is on the whole sequence, not a subcomponent)
- The `refobjsubid` is not 0 (dependency targets a specific column)
- The dependency type is either DEPENDENCY_AUTO or DEPENDENCY_INTERNAL
- The dependent object is confirmed to be a sequence via `get_rel_relkind`

If a `deptype` filter is specified, only sequences with that exact dependency type are included in the results. The function builds and returns a list containing the OIDs of all matching sequences.

## Parameters / Member Variables
- `relid`: The OID of the table whose owned sequences should be found
- `attnum`: The column number to search for (0 means search all columns of the table)
- `deptype`: Optional filter for dependency type (0 means include both AUTO and INTERNAL dependencies)

## Dependencies
- Functions called/Symbols referenced:
  - [SysScanDesc](../S/SysScanDesc.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - Form_pg_depend
  - DEPENDENCY_AUTO
  - DEPENDENCY_INTERNAL
  - [get_rel_relkind](get_rel_relkind.md)
  - RELKIND_SEQUENCE
  - [lappend_oid](../l/lappend_oid.md)
- Called from (representative examples):
  - [getOwnedSequences](getOwnedSequences.md)
  - [getIdentitySequence](getIdentitySequence.md)

## Notes and Other Information
- This is a static function, only accessible within the pg_depend.c file
- The function includes a relkind check to ensure dependent objects are actually sequences, since indexes can also have auto dependencies on columns
- Returns NIL (empty list) if no owned sequences are found
- Uses the DependReferenceIndexId index for efficient scanning by referenced object
- The distinction between AUTO and INTERNAL dependencies reflects different types of sequence ownership (e.g., SERIAL vs IDENTITY columns)
- This function is the foundation for sequence ownership tracking in PostgreSQL's dependency system

## Simplified Source

```c
static List *
getOwnedSequences_internal(Oid relid, AttrNumber attnum, char deptype) {
    List *result = NIL;
    Relation depRel;
    ScanKeyData key[3];
    SysScanDesc scan;
    HeapTuple tup;

    // Open pg_depend table for scanning
    depRel = table_open(DependRelationId, AccessShareLock);

    // Set up scan keys for dependency lookup
    ScanKeyInit(&key[0], Anum_pg_depend_refclassid,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(RelationRelationId));
    ScanKeyInit(&key[1], Anum_pg_depend_refobjid,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(relid));

    // Add column-specific key if attnum specified
    if (attnum)
        ScanKeyInit(&key[2], Anum_pg_depend_refobjsubid,
                    BTEqualStrategyNumber, F_INT4EQ,
                    Int32GetDatum(attnum));

    // Begin scan using appropriate number of keys
    scan = systable_beginscan(depRel, DependReferenceIndexId, true,
                             NULL, attnum ? 3 : 2, key);

    // Process each dependency record
    while (HeapTupleIsValid(tup = systable_getnext(scan))) {
        Form_pg_depend deprec = (Form_pg_depend) GETSTRUCT(tup);

        // Check if this is a sequence owned by the table/column
        if (deprec->classid == RelationRelationId &&
            deprec->objsubid == 0 &&                    // Whole sequence
            deprec->refobjsubid != 0 &&                 // Specific column
            (deprec->deptype == DEPENDENCY_AUTO ||
             deprec->deptype == DEPENDENCY_INTERNAL) &&
            get_rel_relkind(deprec->objid) == RELKIND_SEQUENCE) {

            // Add to results if matches dependency type filter
            if (!deptype || deprec->deptype == deptype)
                result = lappend_oid(result, deprec->objid);
        }
    }

    // Clean up
    systable_endscan(scan);
    table_close(depRel, AccessShareLock);

    return result;
}
```