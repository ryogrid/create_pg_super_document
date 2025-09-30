# ObjectsInPublicationToOids

## Location
[src/backend/commands/publicationcmds.c:166-218](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/publicationcmds.c#L166-L218)

## Overview
Converts a list of PublicationObjSpec objects into separate lists of table OIDs and schema OIDs for publication processing.

## Definition

```c
static void
ObjectsInPublicationToOids(List *pubobjspec_list, ParseState *pstate,
						   List **rels, List **schemas)
```
## Detailed Description
This function processes a list of PublicationObjSpec objects that represent different types of publication targets (individual tables, all tables in specific schemas, or all tables in the current schema) and separates them into two output lists: one for table relations and one for schema OIDs. It handles three publication object types: PUBLICATIONOBJ_TABLE for individual tables, PUBLICATIONOBJ_TABLES_IN_SCHEMA for all tables in named schemas, and PUBLICATIONOBJ_TABLES_IN_CUR_SCHEMA for all tables in the current schema from the search path.

The function automatically filters out duplicate schema OIDs when the same schema is specified multiple times. For PUBLICATIONOBJ_TABLES_IN_CUR_SCHEMA, it uses the search path to resolve the current schema, reporting an error if no valid schema is found in the search path.

## Parameters / Member Variables
- : Input list of PublicationObjSpec objects to process
- : ParseState context for error reporting (currently unused in function body)
- : Output parameter - pointer to list that will contain PublicationTable objects for individual tables
- : Output parameter - pointer to list that will contain schema OIDs for schema-based publications

## Dependencies
- Functions called/Symbols referenced:
  - [get_namespace_oid](../g/get_namespace_oid.md)
  - [list_append_unique_oid](../l/list_append_unique_oid.md)
  - [fetch_search_path](../f/fetch_search_path.md)
  - linitial_oid
  - [list_free](../l/list_free.md)
  - [PublicationObjSpec](../P/PublicationObjSpec.md)
  - PUBLICATIONOBJ_TABLE
  - PUBLICATIONOBJ_TABLES_IN_SCHEMA
  - PUBLICATIONOBJ_TABLES_IN_CUR_SCHEMA
- Called from (representative examples):
  - [CreatePublication](../C/CreatePublication.md)
  - [AlterPublication](../A/AlterPublication.md)

## Notes and Other Information
- Returns early without processing if pubobjspec_list is NULL
- Automatically deduplicates schema OIDs when the same schema is specified multiple times
- For PUBLICATIONOBJ_TABLES_IN_CUR_SCHEMA, uses the first schema in the search path as the current schema
- Provides specific error handling for cases where no valid schema exists in the search path
- Uses elog(ERROR) for unexpected publication object types, indicating an internal error condition
- Located in src/backend/commands/publicationcmds.c:166-218

## Simplified Source

```c
static void ObjectsInPublicationToOids(List *pubobjspec_list, ParseState *pstate,
                                      List **rels, List **schemas) {
    ListCell *cell;
    PublicationObjSpec *pubobj;

    if (!pubobjspec_list)
        return;

    foreach(cell, pubobjspec_list) {
        pubobj = (PublicationObjSpec *) lfirst(cell);

        switch (pubobj->pubobjtype) {
            case PUBLICATIONOBJ_TABLE:
                // Add individual table to relations list
                *rels = lappend(*rels, pubobj->pubtable);
                break;

            case PUBLICATIONOBJ_TABLES_IN_SCHEMA:
                // Add schema to schemas list (with deduplication)
                Oid schemaid = get_namespace_oid(pubobj->name, false);
                *schemas = list_append_unique_oid(*schemas, schemaid);
                break;

            case PUBLICATIONOBJ_TABLES_IN_CUR_SCHEMA:
                // Use first schema from search path as current schema
                List *search_path = fetch_search_path(false);
                if (search_path == NIL) {
                    ereport(ERROR, (errcode(ERRCODE_UNDEFINED_SCHEMA),
                                   errmsg("no schema has been selected for CURRENT_SCHEMA")));
                }

                Oid current_schemaid = linitial_oid(search_path);
                list_free(search_path);
                *schemas = list_append_unique_oid(*schemas, current_schemaid);
                break;

            default:
                elog(ERROR, "invalid publication object type %d", pubobj->pubobjtype);
                break;
        }
    }
}
```