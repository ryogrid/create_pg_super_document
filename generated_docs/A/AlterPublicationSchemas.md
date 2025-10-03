# AlterPublicationSchemas

## Location
[src/backend/commands/publicationcmds.c:1249-1332](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/publicationcmds.c#L1249-L1332)

## Overview
AlterPublicationSchemas handles adding, removing, or replacing schemas in a publication, enforcing constraints related to existing table column lists and managing schema locks.

## Definition

```c
static void
AlterPublicationSchemas(AlterPublicationStmt *stmt,
						HeapTuple tup, List *schemaidlist)
```
## Detailed Description
AlterPublicationSchemas is a static function that manages schema membership in publications based on the specified action (ADD, DROP, or SET). The function enforces the important constraint that schemas cannot be added to publications that already contain tables with column lists, as this combination is not supported. For ADD operations, it validates existing table configurations before adding schemas. For DROP operations, it removes specified schemas. For SET operations, it calculates the difference between existing and new schema lists and performs the necessary additions and removals.

The function maintains proper schema locking throughout the operation to prevent concurrent schema modifications that could lead to inconsistent states. It integrates with the publication system's schema management functions and handles edge cases like empty schema lists in SET operations.

## Parameters / Member Variables
- `*stmt`: AlterPublicationStmt containing the alteration command details and action type
- `tup`: HeapTuple representing the publication record being modified
- `*schemaidlist`: List of schema OIDs to be processed (can be NULL for SET operations that remove all schemas)
## Dependencies
- Functions called/Symbols referenced:
  - [LockSchemaList](../L/LockSchemaList.md): Acquires locks on schemas to prevent concurrent modifications
  - [GetPublicationRelations](../G/GetPublicationRelations.md): Retrieves existing publication relations for validation
  - [SearchSysCache2](../S/SearchSysCache2.md): Searches system cache for publication-relation mappings
  - [heap_attisnull](../h/heap_attisnull.md): Checks if column list attributes are NULL in publication relations
  - [PublicationAddSchemas](../P/PublicationAddSchemas.md)/PublicationDropSchemas: Core functions for adding/removing schemas from publications
  - [GetPublicationSchemas](../G/GetPublicationSchemas.md): Retrieves existing schema OIDs for the publication
  - [list_difference_oid](../l/list_difference_oid.md): Calculates differences between schema OID lists
- Called from (representative examples):
  - [AlterPublication](AlterPublication.md): Main function handling publication alterations

## Notes and Other Information
- Enforces the constraint that schemas cannot be added when tables with column lists exist in the publication
- Provides detailed error messages explaining why schema addition is blocked
- Handles three operation types: AP_AddObjects, AP_DropObjects, and AP_SetObjects
- Uses proper schema locking to prevent race conditions during concurrent operations
- SET operations intelligently calculate differences to minimize unnecessary changes
- Integrates with the publication system's validation framework
- Handles edge cases where schema lists may be empty (particularly for SET operations)
- Maintains consistency with existing table-based publication configurations
- Uses ignore_if_exists flags appropriately for SET operations to handle duplicates gracefully

## Simplified Source

```c
static void AlterPublicationSchemas(AlterPublicationStmt *stmt, HeapTuple tup, List *schemaidlist) {
    Form_pg_publication pubform = (Form_pg_publication) GETSTRUCT(tup);

    // Skip if no schemas specified (except for SET operations)
    if (!schemaidlist && stmt->action != AP_SetObjects)
        return;

    // Lock schemas to prevent concurrent modifications
    LockSchemaList(schemaidlist);

    if (stmt->action == AP_AddObjects) {
        // Validate that no tables with column lists exist before adding schemas
        List *reloids = GetPublicationRelations(pubform->oid, PUBLICATION_PART_ROOT);

        foreach(lc, reloids) {
            HeapTuple coltuple = SearchSysCache2(PUBLICATIONRELMAP,
                                                ObjectIdGetDatum(lfirst_oid(lc)),
                                                ObjectIdGetDatum(pubform->oid));
            if (HeapTupleIsValid(coltuple)) {
                // Check if table has column list defined
                if (!heap_attisnull(coltuple, Anum_pg_publication_rel_prattrs, NULL)) {
                    ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                   errmsg("cannot add schema to publication \"%s\"", stmt->pubname),
                                   errdetail("Schemas cannot be added if any tables that specify a column list are already part of the publication.")));
                }
                ReleaseSysCache(coltuple);
            }
        }

        // Add the schemas to publication
        PublicationAddSchemas(pubform->oid, schemaidlist, false, stmt);
    }
    else if (stmt->action == AP_DropObjects) {
        // Remove schemas from publication
        PublicationDropSchemas(pubform->oid, schemaidlist, false);
    }
    else { // AP_SetObjects
        // Replace existing schemas with new list
        List *oldschemaids = GetPublicationSchemas(pubform->oid);
        List *delschemas = list_difference_oid(oldschemaids, schemaidlist);

        // Lock and remove schemas not in new list
        LockSchemaList(delschemas);
        PublicationDropSchemas(pubform->oid, delschemas, true);

        // Add new schemas (duplicates will be skipped)
        PublicationAddSchemas(pubform->oid, schemaidlist, true, stmt);
    }
}
```