# find_typed_table_dependencies

## Location
[src/backend/commands/tablecmds.c:6896-6944](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L6896-L6944)

## Overview
Identifies typed tables that depend on a specified composite type and either returns their list or raises an error based on the specified drop behavior.

## Definition
```c
static List *find_typed_table_dependencies(Oid typeOid, const char *typeName, DropBehavior behavior)
```

## Detailed Description
This function searches for typed tables that use a specific composite type as their row type by scanning the pg_class system catalog. Typed tables are tables created with the OF clause that inherit their structure from a composite type. The function's behavior depends on the DropBehavior parameter: if RESTRICT is specified, it immediately raises an error when any dependent typed tables are found; otherwise, it collects and returns a list of the OIDs of all dependent typed tables.

The function performs a catalog scan on pg_class using the reloftype attribute to find relations that are typed with the specified type OID. This is essential for operations like ALTER TYPE that need to either prevent changes when dependent objects exist (RESTRICT behavior) or propagate changes to dependent typed tables (CASCADE behavior).

## Parameters / Member Variables
- `typeOid`: The OID of the composite type to check for typed table dependencies
- `typeName`: The name of the type, used in error messages when RESTRICT behavior is specified
- `behavior`: The drop behavior determining whether to error (DROP_RESTRICT) or collect dependencies

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md)
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [table_beginscan_catalog](../t/table_beginscan_catalog.md)
  - [heap_getnext](../h/heap_getnext.md)
  - [table_endscan](../t/table_endscan.md)
  - [table_close](../t/table_close.md)
  - [lappend_oid](../l/lappend_oid.md)
  - ereport
  - Form_pg_class
  - DropBehavior
  - DROP_RESTRICT
- Called from (representative examples):
  - child_dependency_type
  - [renameatt_internal](../r/renameatt_internal.md)
  - [ATTypedTableRecursion](../A/ATTypedTableRecursion.md)

## Notes and Other Information
- The function is static, indicating it's only used within the tablecmds.c file
- Returns NIL (empty list) if no typed tables depend on the specified type
- The error message suggests using CASCADE to handle dependent typed tables
- Uses AccessShareLock for safe concurrent access to the pg_class catalog
- Typed tables are a PostgreSQL feature allowing tables to inherit structure from composite types

## Simplified Source

```c
static List *
find_typed_table_dependencies(Oid typeOid, const char *typeName, DropBehavior behavior)
{
    List *result = NIL;

    // Open pg_class catalog for scanning
    Relation classRel = table_open(RelationRelationId, AccessShareLock);

    // Set up scan key to find tables with reloftype = typeOid
    ScanKeyData key[1];
    ScanKeyInit(&key[0], Anum_pg_class_reloftype, BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(typeOid));

    // Scan for typed tables that use this composite type
    TableScanDesc scan = table_beginscan_catalog(classRel, 1, key);
    HeapTuple tuple;

    while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL) {
        Form_pg_class classform = (Form_pg_class) GETSTRUCT(tuple);

        // If RESTRICT behavior, error on any dependency found
        if (behavior == DROP_RESTRICT)
            ereport(ERROR,
                    (errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
                     errmsg("cannot alter type \"%s\" because it is the type of a typed table",
                            typeName),
                     errhint("Use ALTER ... CASCADE to alter the typed tables too.")));

        // Otherwise, collect the table OID for later processing
        result = lappend_oid(result, classform->oid);
    }

    // Clean up scan and release lock
    table_endscan(scan);
    table_close(classRel, AccessShareLock);

    return result;
}
```