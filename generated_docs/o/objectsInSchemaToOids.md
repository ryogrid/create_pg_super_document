# objectsInSchemaToOids

## Location
[src/backend/catalog/aclchk.c:849-937](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/aclchk.c#L849-L937)

## Overview
Finds all objects of a specified type within given schemas and returns a list of their OIDs, with USAGE privilege checking on the schemas but no privilege checking on individual objects.

## Definition

```c
static List *
objectsInSchemaToOids(ObjectType objtype, List *nspnames)
```
## Detailed Description
This function iterates through a list of schema names and collects all objects of a specified type from those schemas. It performs USAGE privilege checks on each schema via LookupExplicitNamespace but does not check privileges on the individual objects found. The function handles different object types including tables (regular tables, views, materialized views, foreign tables, partitioned tables), sequences, and callable objects (functions, procedures, routines). For relation-based objects, it uses getRelationsInNamespace to efficiently retrieve objects by relation kind. For functions and procedures, it performs a catalog scan on pg_proc with appropriate filtering based on the prokind attribute.

## Parameters / Member Variables
- : The type of database object to search for (OBJECT_TABLE, OBJECT_SEQUENCE, OBJECT_FUNCTION, OBJECT_PROCEDURE, or OBJECT_ROUTINE)
- : A list of schema names (as String values) to search within

## Dependencies
- Functions called/Symbols referenced:
  - [LookupExplicitNamespace](../L/LookupExplicitNamespace.md)
  - [getRelationsInNamespace](../g/getRelationsInNamespace.md)
  - [list_concat](../l/list_concat.md)
  - [lappend_oid](../l/lappend_oid.md)
  - [table_open](../t/table_open.md)
  - [table_beginscan_catalog](../t/table_beginscan_catalog.md)
  - [heap_getnext](../h/heap_getnext.md)
  - [table_endscan](../t/table_endscan.md)
  - [table_close](../t/table_close.md)
- Called from (representative examples):
  - InternalDefaultACL
  - [ExecuteGrantStmt](../E/ExecuteGrantStmt.md)

## Notes and Other Information
- The function is static and used internally within aclchk.c for ACL-related operations
- For OBJECT_TABLE, it collects multiple relation kinds: regular tables, views, materialized views, foreign tables, and partitioned tables
- For callable objects (functions/procedures), it distinguishes between different prokind values to filter appropriately
- OBJECT_ROUTINE includes both functions and procedures without filtering by prokind
- Error handling includes an elog(ERROR) for unrecognized object types
- The function concatenates results from multiple schemas into a single list

## Simplified Source

```c
static List *objectsInSchemaToOids(ObjectType objtype, List *nspnames) {
    List *objects = NIL;
    ListCell *cell;

    // Process each schema name
    foreach(cell, nspnames) {
        char *nspname = strVal(lfirst(cell));
        Oid namespaceId;
        List *objs;

        // Look up schema and check USAGE privilege
        namespaceId = LookupExplicitNamespace(nspname, false);

        // Collect objects based on type
        switch (objtype) {
            case OBJECT_TABLE:
                // Get all table-like objects (tables, views, etc.)
                objs = getRelationsInNamespace(namespaceId, RELKIND_RELATION);
                objects = list_concat(objects, objs);
                objs = getRelationsInNamespace(namespaceId, RELKIND_VIEW);
                objects = list_concat(objects, objs);
                objs = getRelationsInNamespace(namespaceId, RELKIND_MATVIEW);
                objects = list_concat(objects, objs);
                objs = getRelationsInNamespace(namespaceId, RELKIND_FOREIGN_TABLE);
                objects = list_concat(objects, objs);
                objs = getRelationsInNamespace(namespaceId, RELKIND_PARTITIONED_TABLE);
                objects = list_concat(objects, objs);
                break;

            case OBJECT_SEQUENCE:
                // Get sequences
                objs = getRelationsInNamespace(namespaceId, RELKIND_SEQUENCE);
                objects = list_concat(objects, objs);
                break;

            case OBJECT_FUNCTION:
            case OBJECT_PROCEDURE:
            case OBJECT_ROUTINE:
                {
                    // Scan pg_proc catalog with appropriate filtering
                    ScanKeyData key[2];
                    int keycount = 0;
                    Relation rel;
                    TableScanDesc scan;
                    HeapTuple tuple;

                    // Filter by namespace
                    ScanKeyInit(&key[keycount++], Anum_pg_proc_pronamespace,
                              BTEqualStrategyNumber, F_OIDEQ,
                              ObjectIdGetDatum(namespaceId));

                    // Add prokind filter if needed
                    if (objtype == OBJECT_FUNCTION) {
                        // Exclude procedures (include functions, aggregates, window functions)
                        ScanKeyInit(&key[keycount++], Anum_pg_proc_prokind,
                                  BTEqualStrategyNumber, F_CHARNE,
                                  CharGetDatum(PROKIND_PROCEDURE));
                    } else if (objtype == OBJECT_PROCEDURE) {
                        // Include only procedures
                        ScanKeyInit(&key[keycount++], Anum_pg_proc_prokind,
                                  BTEqualStrategyNumber, F_CHAREQ,
                                  CharGetDatum(PROKIND_PROCEDURE));
                    }
                    // OBJECT_ROUTINE includes both functions and procedures

                    // Perform catalog scan
                    rel = table_open(ProcedureRelationId, AccessShareLock);
                    scan = table_beginscan_catalog(rel, keycount, key);

                    while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL) {
                        Oid oid = ((Form_pg_proc) GETSTRUCT(tuple))->oid;
                        objects = lappend_oid(objects, oid);
                    }

                    table_endscan(scan);
                    table_close(rel, AccessShareLock);
                }
                break;

            default:
                elog(ERROR, "unrecognized GrantStmt.objtype: %d", (int) objtype);
        }
    }

    return objects;
}
```