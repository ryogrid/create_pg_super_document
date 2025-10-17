# AssignDumpId

## Location
[src/bin/pg_dump/common.c:646-707](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/common.c#L646-L707)

## Overview
Assigns a unique dump ID to newly created dumpable objects and registers them in pg_dump's internal tracking systems.

## Definition

```c
void
AssignDumpId(DumpableObject *dobj)
```
## Detailed Description
The AssignDumpId function is a central component of pg_dump's object management system that assigns unique sequential identifiers to all dumpable database objects. It performs essential initialization of DumpableObject structures by setting default values for dump options, dependency tracking, and extension membership status. The function maintains two critical data structures: dumpIdMap (an array for fast lookup by ID) and catalogIdHash (a hash table for lookup by PostgreSQL catalog OID).

The function dynamically manages memory allocation for the dumpIdMap array, doubling its size when needed to accommodate new objects. For objects with valid catalog IDs (most database objects), it also enters them into a hash table that enables fast lookup during dependency resolution and cross-reference operations throughout the dump process.

## Parameters / Member Variables
- `*dobj`: Pointer to a DumpableObject that needs a dump ID assigned. The caller must have initialized objType and catId fields before calling this function.
## Dependencies
- Functions called/Symbols referenced:
  - pg_malloc_array (allocates initial dumpIdMap array)
  - pg_realloc_array (expands dumpIdMap when needed)
  - catalogid_create (creates catalog ID hash table)
  - catalogid_insert (inserts objects into catalog hash table)
  - CATALOGIDHASH_INITIAL_SIZE (initial hash table size constant)
- Called from (representative examples):
  - [flagInhTables](../f/flagInhTables.md) (src/bin/pg_dump/common.c:384)
  - [flagInhIndexes](../f/flagInhIndexes.md) (src/bin/pg_dump/common.c:441)
  - [flagInhAttrs](../f/flagInhAttrs.md) (src/bin/pg_dump/common.c:602)
  - [getNamespaces](../g/getNamespaces.md) (src/bin/pg_dump/pg_dump.c:5682)
  - [getTypes](../g/getTypes.md) (src/bin/pg_dump/pg_dump.c:5921)
  - [getIndexes](../g/getIndexes.md) (src/bin/pg_dump/pg_dump.c:7664)

## Notes and Other Information
The function maintains a global lastDumpId counter that ensures each object receives a unique sequential identifier. Default initialization assumes objects should be dumped completely (DUMP_COMPONENT_ALL) and are not extension members. The dumpIdMap array uses a doubling growth strategy starting at 256 entries to balance memory usage with allocation overhead.

Objects are entered into the catalogIdHash only if they have valid PostgreSQL catalog OIDs, as some synthetic objects (like TableAttachInfo) don't correspond to actual catalog entries. The function is called for virtually every dumpable object encountered during the schema collection phase.

## Simplified Source

```c
void AssignDumpId(DumpableObject *dobj) {
    // Assign unique sequential dump ID
    dobj->dumpId = ++lastDumpId;

    // Initialize standard fields with defaults
    dobj->name = NULL;                           // must be set later
    dobj->namespace = NULL;                      // may be set later
    dobj->dump = DUMP_COMPONENT_ALL;             // default assumption
    dobj->dump_contains = DUMP_COMPONENT_ALL;    // default assumption
    dobj->components = DUMP_COMPONENT_DEFINITION; // all objects have definitions
    dobj->ext_member = false;                    // default assumption
    dobj->depends_on_ext = false;                // default assumption
    dobj->dependencies = NULL;
    dobj->nDeps = 0;
    dobj->allocDeps = 0;

    // Expand dumpIdMap array if needed (double-sizing strategy)
    while (dobj->dumpId >= allocedDumpIds) {
        int newAlloc;

        if (allocedDumpIds <= 0) {
            newAlloc = 256;
            dumpIdMap = pg_malloc_array(DumpableObject *, newAlloc);
        } else {
            newAlloc = allocedDumpIds * 2;
            dumpIdMap = pg_realloc_array(dumpIdMap, DumpableObject *, newAlloc);
        }

        // Clear new entries
        memset(dumpIdMap + allocedDumpIds, 0,
               (newAlloc - allocedDumpIds) * sizeof(DumpableObject *));
        allocedDumpIds = newAlloc;
    }

    // Register object in dump ID map
    dumpIdMap[dobj->dumpId] = dobj;

    // Enter into catalog hash table if it has a valid catalog OID
    if (OidIsValid(dobj->catId.tableoid)) {
        CatalogIdMapEntry *entry;
        bool found;

        // Initialize catalog hash table if not done yet
        if (catalogIdHash == NULL)
            catalogIdHash = catalogid_create(CATALOGIDHASH_INITIAL_SIZE, NULL);

        entry = catalogid_insert(catalogIdHash, dobj->catId, &found);
        if (!found) {
            entry->dobj = NULL;
            entry->ext = NULL;
        }
        Assert(entry->dobj == NULL);
        entry->dobj = dobj;
    }
}
```