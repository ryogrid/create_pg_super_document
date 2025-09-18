# pub_collist_to_bitmapset

## Location
src/backend/catalog/pg_publication.c: 570 - 605

## Overview
Transforms a column list represented as an array Datum (stored in catalog) into a Bitmapset for efficient column membership testing and operations.

## Definition
```c
Bitmapset *pub_collist_to_bitmapset(Bitmapset *columns, Datum pubcols, MemoryContext mcxt)
```

## Detailed Description
This utility function converts a publication column list from its catalog storage format (array of int16 attribute numbers) into a Bitmapset data structure. The Bitmapset representation allows for efficient set operations like membership testing, unions, and intersections that are commonly needed when working with column lists.

The function can operate in two modes:
1. Create a new Bitmapset from scratch (when columns parameter is NULL)
2. Add column numbers to an existing Bitmapset (when columns parameter is provided)

The function also supports memory context management - if a specific MemoryContext is provided, the Bitmapset operations will be performed in that context, ensuring proper memory allocation lifetime management.

The input Datum represents an int16 array stored in the catalog (typically from pg_publication_rel.prattrs column), which contains the attribute numbers of columns included in the publication.

## Parameters / Member Variables
- `columns`: Existing Bitmapset to extend, or NULL to create a new one
- `pubcols`: Datum containing an array of int16 attribute numbers from catalog storage
- `mcxt`: Memory context to use for Bitmapset operations, or NULL to use current context

## Dependencies
- Functions called/Symbols referenced:
  - DatumGetArrayTypeP
  - ARR_DIMS
  - ARR_DATA_PTR
  - [bms_add_member](../b/bms_add_member.md)
- Called from (representative examples):
  - [pub_collist_contains_invalid_column](pub_collist_contains_invalid_column.md) (src/backend/commands/publicationcmds.c:382)
  - [AlterPublicationTables](../A/AlterPublicationTables.md) (src/backend/commands/publicationcmds.c:1164)
  - [pgoutput_column_list_init](pgoutput_column_list_init.md) (src/backend/replication/pgoutput/pgoutput.c:1106)

## Notes and Other Information
- The function handles memory context switching properly, restoring the original context after operations
- If no existing Bitmapset is provided, a new one is created and populated
- The input array is expected to contain int16 values representing attribute numbers
- This is a key utility function for converting between catalog storage format and runtime data structures
- Used extensively in publication management commands and logical replication output processing
- The function assumes the input Datum is a valid array - no validation is performed on the array format
- Returns either a new Bitmapset or the modified existing one depending on the columns parameter
- Location: src/backend/catalog/pg_publication.c:570-605