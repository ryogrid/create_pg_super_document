# AlterPublicationOptions

## Location
[src/backend/commands/publicationcmds.c:871-1057](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/publicationcmds.c#L871-L1057)

## Overview
AlterPublicationOptions modifies the options of an existing publication, handling changes to publish actions and partition root publishing preferences while enforcing constraints related to WHERE clauses and column lists.

## Definition

```c
static void
AlterPublicationOptions(ParseState *pstate, AlterPublicationStmt *stmt,
						Relation rel, HeapTuple tup)
```
## Detailed Description
AlterPublicationOptions is a static function that handles the modification of publication options such as publish actions (insert, update, delete, truncate) and the publish_via_partition_root setting. The function performs comprehensive validation to ensure that certain combinations of settings are not allowed, particularly when disabling publish_via_partition_root for publications containing partitioned tables with WHERE clauses or column lists.

The function parses the new options, validates constraints (especially for partitioned tables), updates the catalog tuple, and invalidates the appropriate relation cache entries. It includes sophisticated logic to handle partition hierarchies and ensures consistency between publication options and existing table configurations.

## Parameters / Member Variables
- : ParseState containing parsing context and source text information
- : AlterPublicationStmt structure containing the alteration command details
- : Relation object for the pg_publication catalog table
- : HeapTuple representing the existing publication record to be modified

## Dependencies
- Functions called/Symbols referenced:
  - [parse_publication_options](../p/parse_publication_options.md): Parses publication-specific options from the statement
  - [LockDatabaseObject](../L/LockDatabaseObject.md): Locks the publication to prevent concurrent modifications
  - [GetPublicationRelations](../G/GetPublicationRelations.md): Retrieves relations associated with the publication
  - [heap_attisnull](../h/heap_attisnull.md): Checks for NULL values in tuple attributes (for WHERE clauses and column lists)
  - [get_rel_relkind](../g/get_rel_relkind.md)/get_rel_name: Retrieves relation metadata for validation
  - [heap_modify_tuple](../h/heap_modify_tuple.md): Creates a modified version of the publication tuple
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md): Updates the publication record in the catalog
  - [InvalidatePublicationRels](../I/InvalidatePublicationRels.md): Invalidates relation cache entries for affected tables
  - [GetAllSchemaPublicationRelations](../G/GetAllSchemaPublicationRelations.md): Gets schema-based publication relations
- Called from (representative examples):
  - [AlterPublication](AlterPublication.md): Main function handling publication alterations

## Notes and Other Information
- Enforces the constraint that partitioned tables with WHERE clauses or column lists cannot exist when publish_via_partition_root is false
- Handles both explicit table publications and schema-based publications
- Performs sophisticated partition tree traversal to invalidate all affected relations
- Uses system cache lookups to validate existing publication-relation mappings
- Includes comprehensive error reporting for constraint violations
- Supports event trigger integration for DDL command tracking
- Handles concurrent table drops gracefully by checking for NULL relation names

## Simplified Source

```c
static void AlterPublicationOptions(ParseState *pstate, AlterPublicationStmt *stmt,
                                   Relation rel, HeapTuple tup) {
    bool nulls[Natts_pg_publication];
    bool replaces[Natts_pg_publication];
    Datum values[Natts_pg_publication];
    bool publish_given;
    PublicationActions pubactions;
    bool publish_via_partition_root_given;
    bool publish_via_partition_root;
    Form_pg_publication pubform;

    // Parse the new publication options
    parse_publication_options(pstate, stmt->options, &publish_given, &pubactions,
                             &publish_via_partition_root_given, &publish_via_partition_root);

    pubform = (Form_pg_publication) GETSTRUCT(tup);

    // Validate constraints for partitioned tables when disabling publish_via_partition_root
    if (!pubform->puballtables && publish_via_partition_root_given && !publish_via_partition_root) {
        LockDatabaseObject(PublicationRelationId, pubform->oid, 0, AccessShareLock);
        List *root_relids = GetPublicationRelations(pubform->oid, PUBLICATION_PART_ROOT);

        foreach(lc, root_relids) {
            Oid relid = lfirst_oid(lc);
            HeapTuple rftuple = SearchSysCache2(PUBLICATIONRELMAP,
                                               ObjectIdGetDatum(relid),
                                               ObjectIdGetDatum(pubform->oid));
            if (!HeapTupleIsValid(rftuple))
                continue;

            // Check for WHERE clauses and column lists on partitioned tables
            bool has_rowfilter = !heap_attisnull(rftuple, Anum_pg_publication_rel_prqual, NULL);
            bool has_collist = !heap_attisnull(rftuple, Anum_pg_publication_rel_prattrs, NULL);

            if ((has_rowfilter || has_collist) && get_rel_relkind(relid) == RELKIND_PARTITIONED_TABLE) {
                char *relname = get_rel_name(relid);
                if (relname) {
                    ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                   errmsg("cannot set parameter \"publish_via_partition_root\" to false"),
                                   errdetail("Partitioned table \"%s\" has WHERE clause or column list",
                                           relname)));
                }
            }
            ReleaseSysCache(rftuple);
        }
    }

    // Prepare new tuple values
    memset(values, 0, sizeof(values));
    memset(nulls, false, sizeof(nulls));
    memset(replaces, false, sizeof(replaces));

    // Update publish actions if specified
    if (publish_given) {
        values[Anum_pg_publication_pubinsert - 1] = BoolGetDatum(pubactions.pubinsert);
        replaces[Anum_pg_publication_pubinsert - 1] = true;

        values[Anum_pg_publication_pubupdate - 1] = BoolGetDatum(pubactions.pubupdate);
        replaces[Anum_pg_publication_pubupdate - 1] = true;

        values[Anum_pg_publication_pubdelete - 1] = BoolGetDatum(pubactions.pubdelete);
        replaces[Anum_pg_publication_pubdelete - 1] = true;

        values[Anum_pg_publication_pubtruncate - 1] = BoolGetDatum(pubactions.pubtruncate);
        replaces[Anum_pg_publication_pubtruncate - 1] = true;
    }

    // Update publish_via_partition_root if specified
    if (publish_via_partition_root_given) {
        values[Anum_pg_publication_pubviaroot - 1] = BoolGetDatum(publish_via_partition_root);
        replaces[Anum_pg_publication_pubviaroot - 1] = true;
    }

    // Update the catalog with new values
    tup = heap_modify_tuple(tup, RelationGetDescr(rel), values, nulls, replaces);
    CatalogTupleUpdate(rel, &tup->t_self, tup);
    CommandCounterIncrement();

    // Invalidate cached relation information
    pubform = (Form_pg_publication) GETSTRUCT(tup);
    if (pubform->puballtables) {
        CacheInvalidateRelcacheAll();
    } else {
        List *relids = GetPublicationRelations(pubform->oid, PUBLICATION_PART_ALL);
        List *schemarelids = GetAllSchemaPublicationRelations(pubform->oid, PUBLICATION_PART_ALL);
        relids = list_concat_unique_oid(relids, schemarelids);
        InvalidatePublicationRels(relids);
    }

    // Trigger event hooks
    ObjectAddress obj;
    ObjectAddressSet(obj, PublicationRelationId, pubform->oid);
    EventTriggerCollectSimpleCommand(obj, InvalidObjectAddress, (Node *) stmt);
    InvokeObjectPostAlterHook(PublicationRelationId, pubform->oid, 0);
}
```