# RelationParseRelOptions

## Location
[src/backend/utils/cache/relcache.c:464-520](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relcache.c#L464-L520)

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
  - [extractRelOptions](../e/extractRelOptions.md) (extracts and parses relation options from tuple)
  - [GetPgClassDescriptor](../G/GetPgClassDescriptor.md) (provides hardwired pg_class tuple descriptor)
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md) (allocates memory in CacheMemoryContext)
  - VARSIZE (macro for variable-length data size calculation)
- Called from (representative examples):
  - [RelationBuildDesc](RelationBuildDesc.md) (during relation descriptor construction)
  - [RelationReloadIndexInfo](RelationReloadIndexInfo.md) (when reloading index information)

## Notes and Other Information
- Only processes relation kinds that support options (tables, indexes, views, materialized views)
- Uses access method-specific parsing for indexes via rd_indam->amoptions function pointer
- Implements careful memory management by parsing in caller's context then copying to CacheMemoryContext
- Requires that rd_rel and rd_indam (for indexes) are already valid before calling
- Uses hardwired pg_class descriptor to handle bootstrap cases where normal descriptors may not be available
- Sets rd_options to NULL initially and only allocates if options are present

## Simplified Source

```c
static void
RelationParseRelOptions(Relation relation, HeapTuple tuple)
{
    bytea *options;
    amoptions_function amoptsfn;

    relation->rd_options = NULL;

    // Determine appropriate parsing function based on relation kind
    switch (relation->rd_rel->relkind)
    {
        case RELKIND_RELATION:
        case RELKIND_TOASTVALUE:
        case RELKIND_VIEW:
        case RELKIND_MATVIEW:
        case RELKIND_PARTITIONED_TABLE:
            amoptsfn = NULL;  // Use default parsing
            break;

        case RELKIND_INDEX:
        case RELKIND_PARTITIONED_INDEX:
            amoptsfn = relation->rd_indam->amoptions;  // Use AM-specific parsing
            break;

        default:
            return;  // No options supported for this relation kind
    }

    // Extract and parse options from pg_class tuple
    options = extractRelOptions(tuple, GetPgClassDescriptor(), amoptsfn);

    if (options) {
        // Copy parsed options to cache memory context for safety
        relation->rd_options = MemoryContextAlloc(CacheMemoryContext, VARSIZE(options));
        memcpy(relation->rd_options, options, VARSIZE(options));
        pfree(options);
    }
}
```