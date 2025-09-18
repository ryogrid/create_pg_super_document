# AssignDumpId

## Location
src/bin/pg_dump/common.c: 646 - 707

## Overview
Assigns a unique dump ID to newly created dumpable objects and registers them in pg_dump's internal tracking systems.

## Definition


## Detailed Description
The AssignDumpId function is a central component of pg_dump's object management system that assigns unique sequential identifiers to all dumpable database objects. It performs essential initialization of DumpableObject structures by setting default values for dump options, dependency tracking, and extension membership status. The function maintains two critical data structures: dumpIdMap (an array for fast lookup by ID) and catalogIdHash (a hash table for lookup by PostgreSQL catalog OID).

The function dynamically manages memory allocation for the dumpIdMap array, doubling its size when needed to accommodate new objects. For objects with valid catalog IDs (most database objects), it also enters them into a hash table that enables fast lookup during dependency resolution and cross-reference operations throughout the dump process.

## Parameters / Member Variables
- : Pointer to a DumpableObject that needs a dump ID assigned. The caller must have initialized objType and catId fields before calling this function.

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