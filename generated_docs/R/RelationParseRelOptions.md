# RelationParseRelOptions

## Location
src/backend/utils/cache/relcache.c: 464 - 520

## Overview
RelationParseRelOptions extracts and parses pg_class.reloptions into the pre-parsed rd_options field of a relation descriptor for efficient option access.

## Definition
static void RelationParseRelOptions(Relation relation, HeapTuple tuple)

## Detailed Description
RelationParseRelOptions converts the raw reloptions text stored in pg_class into a parsed binary format stored in the relation's rd_options field. The function handles different relation kinds (tables, indexes, views, etc.) and uses appropriate access method-specific parsing functions for indexes. It ensures memory safety by parsing options in the caller's context and then copying the results to CacheMemoryContext to prevent memory leaks.

The function uses a switch statement to determine the appropriate parsing approach based on relation kind, with special handling for indexes that require access method-specific option parsing through the rd_indam->amoptions function pointer.

## Parameters / Member Variables
- : The relation descriptor being initialized, where parsed options will be stored in rd_options
- : The actual pg_class HeapTuple containing the raw reloptions data (not the rd_rel copy)

## Dependencies
- Functions called/Symbols referenced:
  - RELKIND_RELATION, RELKIND_TOASTVALUE, RELKIND_VIEW, RELKIND_MATVIEW (relation kind constants)
  - RELKIND_INDEX, RELKIND_PARTITIONED_INDEX (index relation kind constants)
  - extractRelOptions (extracts and parses relation options from tuple)
  - GetPgClassDescriptor (provides hardwired pg_class tuple descriptor)
  - MemoryContextAlloc (allocates memory in CacheMemoryContext)
  - VARSIZE (macro for variable-length data size calculation)
- Called from (representative examples):
  - RelationBuildDesc (during relation descriptor construction)
  - RelationReloadIndexInfo (when reloading index information)

## Notes and Other Information
- Only processes relation kinds that support options (tables, indexes, views, materialized views)
- Uses access method-specific parsing for indexes via rd_indam->amoptions function pointer
- Implements careful memory management by parsing in caller's context then copying to CacheMemoryContext
- Requires that rd_rel and rd_indam (for indexes) are already valid before calling
- Uses hardwired pg_class descriptor to handle bootstrap cases where normal descriptors may not be available
- Sets rd_options to NULL initially and only allocates if options are present