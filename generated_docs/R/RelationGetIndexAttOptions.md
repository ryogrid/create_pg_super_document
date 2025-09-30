# RelationGetIndexAttOptions

## Location
[src/backend/utils/cache/relcache.c:5896-5956](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relcache.c#L5896-L5956)

## Overview
Retrieves and parses AM/opclass-specific options for an index into binary format, providing cached access to parsed index attribute options.

## Definition

```c
bytea	  **
RelationGetIndexAttOptions(Relation relation, bool copy)
```
## Detailed Description
This function returns AM (Access Method) and opclass-specific options for an index relation in a parsed binary format. It implements a caching mechanism to avoid repeated parsing of the same options. The function first checks if cached options are available in the relation's rd_opcoptions field. If not available, it retrieves the raw option text using get_attoptions() for each attribute and parses them using index_opclass_options(). The parsed options are then cached in the relation's index context for future access.

The function handles memory management carefully, switching to the relation's index context when caching options to ensure proper lifetime management. When copy=false, the function cleans up temporary allocations and returns the cached version.

## Parameters / Member Variables
- : The index relation for which to retrieve attribute options
- : If true, returns a copy of the options array; if false, returns cached options directly

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetNumberOfAttributes
  - [CopyIndexAttOptions](../C/CopyIndexAttOptions.md)
  - [get_attoptions](../g/get_attoptions.md)
  - [index_opclass_options](../i/index_opclass_options.md)
- Called from (representative examples):
  - [index_getprocinfo](../i/index_getprocinfo.md)
  - [get_relation_info](../g/get_relation_info.md)
  - [RelationInitIndexAccessInfo](RelationInitIndexAccessInfo.md)
  - [load_critical_index](../l/load_critical_index.md)

## Notes and Other Information
- Uses criticalRelcachesBuilt flag to avoid circular dependencies during system catalog initialization
- Switches memory context to relation's rd_indexcxt when caching to ensure proper memory lifetime
- The cached options are stored in relation->rd_opcoptions for subsequent access
- Handles cleanup of temporary allocations when copy=false to prevent memory leaks

## Simplified Source

```c
bytea **RelationGetIndexAttOptions(Relation relation, bool copy) {
    bytea **opts = relation->rd_opcoptions;
    int natts = RelationGetNumberOfAttributes(relation);

    // Return cached options if available
    if (opts) {
        return copy ? CopyIndexAttOptions(opts, natts) : opts;
    }

    // Parse opclass options for each attribute
    opts = palloc0(sizeof(*opts) * natts);
    for (int i = 0; i < natts; i++) {
        if (criticalRelcachesBuilt && RelationGetRelid(relation) != AttributeRelidNumIndexId) {
            Datum attoptions = get_attoptions(RelationGetRelid(relation), i + 1);
            opts[i] = index_opclass_options(relation, i + 1, attoptions, false);
            if (attoptions != (Datum) 0) {
                pfree(DatumGetPointer(attoptions));
            }
        }
    }

    // Cache the parsed options in relation context
    MemoryContext oldcxt = MemoryContextSwitchTo(relation->rd_indexcxt);
    relation->rd_opcoptions = CopyIndexAttOptions(opts, natts);
    MemoryContextSwitchTo(oldcxt);

    // Return copy or cached version based on copy flag
    if (copy) {
        return opts;
    }

    // Clean up temporary options and return cached version
    for (int i = 0; i < natts; i++) {
        if (opts[i]) pfree(opts[i]);
    }
    pfree(opts);

    return relation->rd_opcoptions;
}
```