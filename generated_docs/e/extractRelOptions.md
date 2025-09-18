# extractRelOptions

## Location
src/backend/access/common/reloptions.c: 1388 - 1435

## Overview
Extracts and parses relation options from a pg_class tuple, converting them into appropriate bytea format based on the relation kind.

## Definition


## Detailed Description
This low-level function extracts relation options from a pg_class heap tuple and parses them into the appropriate bytea structure based on the relation's kind. It reads the reloptions field from the tuple, determines the relation type from relkind, and calls the appropriate parser function (heap_reloptions, index_reloptions, view_reloptions, etc.). This function is designed for use by the relcache system and other low-level code that needs to process relation options without having access to a relation's cache entry. For index relations, it uses the provided amoptions function pointer to handle access method specific options.

## Parameters / Member Variables
- : HeapTuple from pg_class containing the relation's metadata
- : TupleDesc for pg_class, used to extract the reloptions field
- : Function pointer to index access method's options parser (NULL for non-indexes)

## Dependencies
- Functions called/Symbols referenced:
  - fastgetattr (to extract reloptions field)
  - heap_reloptions (for tables, toast tables, materialized views)
  - partitioned_table_reloptions (for partitioned tables)
  - view_reloptions (for views)
  - index_reloptions (for indexes and partitioned indexes)
  - Form_pg_class
  - RELKIND_* constants
- Called from (representative examples):
  - extract_autovac_opts (autovacuum worker)
  - RelationParseRelOptions (relcache)
  - GET_STRING_RELOPTION (macro)

## Notes and Other Information
- Returns NULL if the tuple has no reloptions or for foreign tables
- Does not error out during parsing (false parameter passed to parser functions)
- Foreign tables return NULL since their options are handled separately
- This is preferred over accessing rd_options when relcache entry is not available
- Each relation kind has its own specific options parser to handle different option sets
- Function is defined in src/backend/access/common/reloptions.c:1388-1435