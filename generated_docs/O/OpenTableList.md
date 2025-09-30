# OpenTableList

## Location
[src/backend/commands/publicationcmds.c:1549-1698](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/publicationcmds.c#L1549-L1698)

## Overview
Opens and locks relations specified by a PublicationTable list, preparing them for addition to a publication with proper validation and inheritance handling.

## Definition

```c
static List *
OpenTableList(List *tables)
```
## Detailed Description
OpenTableList is a static function that processes a list of PublicationTable structures to open and lock the corresponding database relations. The function performs several critical tasks:

1. **Relation Opening and Locking**: Opens each specified table with ShareUpdateExclusiveLock to prevent concurrent modifications during publication operations
2. **Duplicate Detection**: Implements an O(N^2) algorithm to filter out duplicate table specifications while ensuring no conflicts exist with WHERE clauses or column lists
3. **Inheritance Handling**: For tables with inheritance (when inh flag is set), automatically includes child tables, except for partitioned tables whose partitions need not be explicitly added
4. **Validation**: Ensures no conflicting WHERE clauses or column lists exist between parent and child tables or duplicate entries

The function returns a list of PublicationRelInfo structures containing the opened relations along with their associated WHERE clauses and column lists.

## Parameters / Member Variables
- : List of PublicationTable structures specifying the tables to be opened, each containing relation information, optional WHERE clauses, and column lists

## Dependencies
- Functions called/Symbols referenced:
  - [table_openrv](../t/table_openrv.md) (opens relation by RangeVar)
  - [find_all_inheritors](../f/find_all_inheritors.md) (finds child tables for inheritance)
  - [list_member_oid](../l/list_member_oid.md) (checks for duplicate OIDs)
  - [lappend](../l/lappend.md)/lappend_oid (list manipulation)
  - RelationGetRelid (gets relation OID)
  - RelationGetRelationName (gets relation name)
- Called from (representative examples):
  - [CreatePublication](../C/CreatePublication.md) (src/backend/commands/publicationcmds.c:831)
  - [AlterPublicationTables](../A/AlterPublicationTables.md) (src/backend/commands/publicationcmds.c:1095)

## Notes and Other Information
- Uses ShareUpdateExclusiveLock to ensure safe concurrent access during publication operations
- The duplicate detection algorithm is O(N^2) but considered acceptable for user-specified table lists
- Inheritance handling excludes partitioned tables as their partitions are handled separately
- Proper error handling for conflicting WHERE clauses and column lists between parent and child tables
- Memory allocation using palloc for PublicationRelInfo structures
- Includes CHECK_FOR_INTERRUPTS() calls to allow query cancellation during long operations

## Simplified Source

```c
static List *OpenTableList(List *tables)
{
    List *relids = NIL;
    List *rels = NIL;
    List *relids_with_rf = NIL;
    List *relids_with_collist = NIL;
    ListCell *lc;

    // Open and lock each explicitly specified relation
    foreach(lc, tables) {
        PublicationTable *t = lfirst_node(PublicationTable, lc);
        bool recurse = t->relation->inh;
        Relation rel;
        Oid myrelid;
        PublicationRelInfo *pub_rel;

        CHECK_FOR_INTERRUPTS();

        rel = table_openrv(t->relation, ShareUpdateExclusiveLock);
        myrelid = RelationGetRelid(rel);

        // Filter out duplicates - O(N^2) but acceptable for user lists
        if (list_member_oid(relids, myrelid)) {
            // Check for conflicts with WHERE clauses
            if (t->whereClause || list_member_oid(relids_with_rf, myrelid)) {
                ereport(ERROR,
                        (errcode(ERRCODE_DUPLICATE_OBJECT),
                         errmsg("conflicting or redundant WHERE clauses for table \"%s\"",
                                RelationGetRelationName(rel))));
            }

            // Check for conflicts with column lists
            if (t->columns || list_member_oid(relids_with_collist, myrelid)) {
                ereport(ERROR,
                        (errcode(ERRCODE_DUPLICATE_OBJECT),
                         errmsg("conflicting or redundant column lists for table \"%s\"",
                                RelationGetRelationName(rel))));
            }

            table_close(rel, ShareUpdateExclusiveLock);
            continue;
        }

        // Create PublicationRelInfo for this relation
        pub_rel = palloc(sizeof(PublicationRelInfo));
        pub_rel->relation = rel;
        pub_rel->whereClause = t->whereClause;
        pub_rel->columns = t->columns;
        rels = lappend(rels, pub_rel);
        relids = lappend_oid(relids, myrelid);

        if (t->whereClause)
            relids_with_rf = lappend_oid(relids_with_rf, myrelid);

        if (t->columns)
            relids_with_collist = lappend_oid(relids_with_collist, myrelid);

        // Add inheritance children if requested
        // (partitioned tables handle partitions separately)
        if (recurse && rel->rd_rel->relkind != RELKIND_PARTITIONED_TABLE) {
            List *children = find_all_inheritors(myrelid, ShareUpdateExclusiveLock, NULL);
            ListCell *child;

            foreach(child, children) {
                Oid childrelid = lfirst_oid(child);

                CHECK_FOR_INTERRUPTS();

                // Skip duplicates between parent and child specifications
                if (list_member_oid(relids, childrelid)) {
                    // Validate no conflicting WHERE clauses
                    if (childrelid != myrelid &&
                        (t->whereClause || list_member_oid(relids_with_rf, childrelid))) {
                        ereport(ERROR,
                                (errcode(ERRCODE_DUPLICATE_OBJECT),
                                 errmsg("conflicting or redundant WHERE clauses for table \"%s\"",
                                        RelationGetRelationName(rel))));
                    }

                    // Validate no conflicting column lists
                    if (childrelid != myrelid &&
                        (t->columns || list_member_oid(relids_with_collist, childrelid))) {
                        ereport(ERROR,
                                (errcode(ERRCODE_DUPLICATE_OBJECT),
                                 errmsg("conflicting or redundant column lists for table \"%s\"",
                                        RelationGetRelationName(rel))));
                    }

                    continue;
                }

                // Add child relation (lock already acquired by find_all_inheritors)
                rel = table_open(childrelid, NoLock);
                pub_rel = palloc(sizeof(PublicationRelInfo));
                pub_rel->relation = rel;
                pub_rel->whereClause = t->whereClause; // Child inherits parent's WHERE clause
                pub_rel->columns = t->columns;         // Child inherits parent's column list
                rels = lappend(rels, pub_rel);
                relids = lappend_oid(relids, childrelid);

                if (t->whereClause)
                    relids_with_rf = lappend_oid(relids_with_rf, childrelid);

                if (t->columns)
                    relids_with_collist = lappend_oid(relids_with_collist, childrelid);
            }
        }
    }

    // Clean up tracking lists
    list_free(relids);
    list_free(relids_with_rf);

    return rels;
}
```