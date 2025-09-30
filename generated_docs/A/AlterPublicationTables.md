# AlterPublicationTables

## Location
[src/backend/commands/publicationcmds.c:1079-1248](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/publicationcmds.c#L1079-L1248)

## Overview
AlterPublicationTables handles adding, removing, or replacing tables in a publication, performing comprehensive validation and maintaining consistency of WHERE clauses and column lists.

## Definition

```c
static void
AlterPublicationTables(AlterPublicationStmt *stmt, HeapTuple tup,
					   List *tables, const char *queryString,
					   bool publish_schema)
```
## Detailed Description
AlterPublicationTables is a static function that manages table membership in publications based on the specified action (ADD, DROP, or SET). For ADD operations, it validates WHERE clauses and column lists before adding tables. For DROP operations, it removes specified tables. For SET operations, it performs a sophisticated comparison between existing and new table lists, preserving tables that match exactly (including WHERE clauses and column lists) and dropping/adding others as needed.

The function handles complex scenarios involving partition hierarchies, schema publications, and ensures that WHERE clauses and column lists are properly validated and transformed. It maintains referential integrity and performs appropriate cache invalidation.

## Parameters / Member Variables
- : AlterPublicationStmt containing the alteration command details and action type
- : HeapTuple representing the publication record being modified
- : List of tables to be processed (can be NULL for SET operations that remove all tables)
- : Original SQL command string used for WHERE clause transformation
- : Boolean indicating if this is related to schema-based publications

## Dependencies
- Functions called/Symbols referenced:
  - [OpenTableList](../O/OpenTableList.md)/CloseTableList: Opens and closes table relations with appropriate locks
  - [TransformPubWhereClauses](../T/TransformPubWhereClauses.md): Transforms and validates WHERE clauses for publication relations
  - [CheckPubRelationColumnList](../C/CheckPubRelationColumnList.md): Validates column list specifications
  - [PublicationAddTables](../P/PublicationAddTables.md)/PublicationDropTables: Core functions for adding/removing tables from publications
  - [GetPublicationRelations](../G/GetPublicationRelations.md): Retrieves existing publication relations
  - [is_schema_publication](../i/is_schema_publication.md): Checks if publication includes schema-based relations
  - [SearchSysCache2](../S/SearchSysCache2.md)/SysCacheGetAttr: System cache operations for existing relation metadata
  - [pub_collist_to_bitmapset](../p/pub_collist_to_bitmapset.md): Converts column lists to bitmap representations
  - [equal](../e/equal.md)/bms_equal: Comparison functions for WHERE clauses and column bitmaps
- Called from (representative examples):
  - [AlterPublication](AlterPublication.md): Main function handling publication alterations

## Notes and Other Information
- Supports three operation types: AP_AddObjects, AP_DropObjects, and AP_SetObjects
- SET operations perform intelligent diffing to minimize unnecessary changes
- Handles complex WHERE clause and column list comparisons during SET operations
- Properly manages table locks (ShareUpdateExclusiveLock) during operations
- Integrates with schema-based publication validation
- Optimizes SET operations by reusing unchanged table-publication relationships
- Handles concurrent table modifications gracefully through system cache operations
- Maintains backward compatibility with existing publication configurations

## Simplified Source

```c
static void AlterPublicationTables(AlterPublicationStmt *stmt, HeapTuple tup,
                                  List *tables, const char *queryString,
                                  bool publish_schema) {
    List *rels = NIL;
    Form_pg_publication pubform = (Form_pg_publication) GETSTRUCT(tup);
    Oid pubid = pubform->oid;

    // Skip if no tables specified (except for SET operations)
    if (!tables && stmt->action != AP_SetObjects)
        return;

    rels = OpenTableList(tables);

    if (stmt->action == AP_AddObjects) {
        // Transform WHERE clauses and validate column lists
        TransformPubWhereClauses(rels, queryString, pubform->pubviaroot);

        publish_schema |= is_schema_publication(pubid);
        CheckPubRelationColumnList(stmt->pubname, rels, publish_schema, pubform->pubviaroot);

        // Add tables to publication
        PublicationAddTables(pubid, rels, false, stmt);
    }
    else if (stmt->action == AP_DropObjects) {
        // Remove tables from publication
        PublicationDropTables(pubid, rels, false);
    }
    else { // AP_SetObjects
        // Replace existing tables with new list
        TransformPubWhereClauses(rels, queryString, pubform->pubviaroot);
        CheckPubRelationColumnList(stmt->pubname, rels, publish_schema, pubform->pubviaroot);

        List *oldrelids = GetPublicationRelations(pubid, PUBLICATION_PART_ROOT);
        List *delrels = NIL;

        // Find tables that need to be dropped (not in new list or with different settings)
        foreach(oldlc, oldrelids) {
            Oid oldrelid = lfirst_oid(oldlc);
            bool found = false;

            // Get existing WHERE clause and column list for comparison
            HeapTuple rftuple = SearchSysCache2(PUBLICATIONRELMAP,
                                               ObjectIdGetDatum(oldrelid),
                                               ObjectIdGetDatum(pubid));

            Node *oldrelwhereclause = NULL;
            Bitmapset *oldcolumns = NULL;

            if (HeapTupleIsValid(rftuple)) {
                // Extract existing WHERE clause and column list
                // (complex extraction logic simplified)
                ReleaseSysCache(rftuple);
            }

            // Check if table exists in new list with same settings
            foreach(newlc, rels) {
                PublicationRelInfo *newpubrel = (PublicationRelInfo *) lfirst(newlc);
                if (RelationGetRelid(newpubrel->relation) == oldrelid) {
                    // Compare WHERE clauses and column lists
                    if (equal(oldrelwhereclause, newpubrel->whereClause) &&
                        bms_equal(oldcolumns, newcolumns)) {
                        found = true;
                        break;
                    }
                }
            }

            // Mark for deletion if not found or settings differ
            if (!found) {
                PublicationRelInfo *oldrel = palloc(sizeof(PublicationRelInfo));
                oldrel->relation = table_open(oldrelid, ShareUpdateExclusiveLock);
                delrels = lappend(delrels, oldrel);
            }
        }

        // Drop old tables and add new ones
        PublicationDropTables(pubid, delrels, true);
        PublicationAddTables(pubid, rels, true, stmt);

        CloseTableList(delrels);
    }

    CloseTableList(rels);
}
```