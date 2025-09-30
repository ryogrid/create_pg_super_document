# extractRelOptions

## Location
[src/backend/access/common/reloptions.c:1388-1435](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/reloptions.c#L1388-L1435)

## Overview
Extracts and parses relation options from a pg_class tuple, converting them into appropriate bytea format based on the relation kind.

## Definition

```c
struct_array_builtin(array, TEXTOID, &optiondatums, NULL, &noptions);
```
## Detailed Description
This low-level function extracts relation options from a pg_class heap tuple and parses them into the appropriate bytea structure based on the relation's kind. It reads the reloptions field from the tuple, determines the relation type from relkind, and calls the appropriate parser function (heap_reloptions, index_reloptions, view_reloptions, etc.). This function is designed for use by the relcache system and other low-level code that needs to process relation options without having access to a relation's cache entry. For index relations, it uses the provided amoptions function pointer to handle access method specific options.

## Parameters / Member Variables
- : HeapTuple from pg_class containing the relation's metadata
- : TupleDesc for pg_class, used to extract the reloptions field
- : Function pointer to index access method's options parser (NULL for non-indexes)

## Dependencies
- Functions called/Symbols referenced:
  - [fastgetattr](../f/fastgetattr.md) (to extract reloptions field)
  - [heap_reloptions](../h/heap_reloptions.md) (for tables, toast tables, materialized views)
  - [partitioned_table_reloptions](../p/partitioned_table_reloptions.md) (for partitioned tables)
  - [view_reloptions](../v/view_reloptions.md) (for views)
  - [index_reloptions](../i/index_reloptions.md) (for indexes and partitioned indexes)
  - Form_pg_class
  - RELKIND_* constants
- Called from (representative examples):
  - [extract_autovac_opts](extract_autovac_opts.md) (autovacuum worker)
  - [RelationParseRelOptions](../R/RelationParseRelOptions.md) (relcache)
  - GET_STRING_RELOPTION (macro)

## Notes and Other Information
- Returns NULL if the tuple has no reloptions or for foreign tables
- Does not error out during parsing (false parameter passed to parser functions)
- Foreign tables return NULL since their options are handled separately
- This is preferred over accessing rd_options when relcache entry is not available
- Each relation kind has its own specific options parser to handle different option sets
- Function is defined in src/backend/access/common/reloptions.c:1388-1435

## Simplified Source

```c
bytea *extractRelOptions(HeapTuple tuple, TupleDesc tupdesc, amoptions_function amoptions) {
    // Extract reloptions field from pg_class tuple
    bool isnull;
    Datum datum = fastgetattr(tuple, Anum_pg_class_reloptions, tupdesc, &isnull);

    if (isnull)
        return NULL;

    // Get relation kind to determine which parser to use
    Form_pg_class classForm = (Form_pg_class) GETSTRUCT(tuple);

    // Parse options based on relation type
    switch (classForm->relkind) {
        case RELKIND_RELATION:
        case RELKIND_TOASTVALUE:
        case RELKIND_MATVIEW:
            return heap_reloptions(classForm->relkind, datum, false);

        case RELKIND_PARTITIONED_TABLE:
            return partitioned_table_reloptions(datum, false);

        case RELKIND_VIEW:
            return view_reloptions(datum, false);

        case RELKIND_INDEX:
        case RELKIND_PARTITIONED_INDEX:
            return index_reloptions(amoptions, datum, false);

        case RELKIND_FOREIGN_TABLE:
            return NULL;  // Foreign tables don't use reloptions

        default:
            return NULL;
    }
}
```