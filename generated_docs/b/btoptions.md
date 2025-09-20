# btoptions

## Location
[src/backend/access/nbtree/nbtutils.c:4563-4585](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtutils.c#L4563-L4585)

## Overview
The btoptions function processes and validates storage parameters (reloptions) specific to B-tree indexes, returning a parsed and validated options structure.

## Definition

```c
bytea *
btoptions(Datum reloptions, bool validate)
```
## Detailed Description
This function is responsible for parsing and validating B-tree index storage parameters. It defines a table of valid options for B-tree indexes including fillfactor, vacuum_cleanup_index_scale_factor, and deduplicate_items. The function uses PostgreSQL's generic reloptions parsing infrastructure to build a BTOptions structure containing the validated parameters.

The function supports three specific B-tree options:
- fillfactor: Controls how full index pages should be during initial build
- vacuum_cleanup_index_scale_factor: Threshold for vacuum cleanup operations 
- deduplicate_items: Whether to enable deduplication of equal keys

## Parameters / Member Variables
- : Datum containing the raw storage options to be parsed
- : Boolean flag indicating whether to perform validation during parsing

## Dependencies
- Functions called/Symbols referenced:
  - [build_reloptions](build_reloptions.md)
  - relopt_parse_elt (structure)
  - BTOptions (structure)
  - RELOPT_TYPE_INT, RELOPT_TYPE_REAL, RELOPT_TYPE_BOOL (constants)
  - RELOPT_KIND_BTREE (constant)
  - lengthof (macro)
- Called from (representative examples):
  - [bthandler](bthandler.md)

## Notes and Other Information
This function is part of PostgreSQL's B-tree access method implementation and is called during index creation/alteration to process storage parameters. The returned bytea structure contains the parsed options that will be stored with the index metadata and used during index operations.