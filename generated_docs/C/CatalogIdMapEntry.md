# CatalogIdMapEntry

## Location
src/bin/pg_dump/common.c: 66 - 67

## Overview
A hash table entry structure used by pg_dump to map PostgreSQL system catalog object identifiers (CatalogId) to their corresponding dump information and extension ownership.

## Definition
```c
typedef struct CatalogIdMapEntry {
    CatalogId       catId;      /* the indexed CatalogId */
    uint32          status;     /* hash status */
    uint32          hashval;    /* hash code for the CatalogId */
    DumpableObject *dobj;       /* the associated DumpableObject, if any */
    ExtensionInfo  *ext;        /* owning extension, if any */
} CatalogIdMapEntry;
```

## Detailed Description
CatalogIdMapEntry serves as a hash table entry in pg_dump's catalog ID mapping system. It provides an efficient way to look up dump objects and extension information based on PostgreSQL system catalog identifiers. This structure is essential for pg_dump's ability to track relationships between database objects and their dump representations, as well as managing extension membership information during the dump process.

The structure uses a hash table implementation (likely simplehash) to provide fast lookups of dump objects by their catalog identifiers. Each entry can optionally reference both a DumpableObject (representing the object to be dumped) and an ExtensionInfo (if the object belongs to an extension).

## Parameters / Member Variables
- `catId`: The PostgreSQL system catalog identifier that serves as the key for this hash table entry
- `status`: Hash table status field used by the hash table implementation for entry state management
- `hashval`: Precomputed hash value for the CatalogId to optimize hash table operations
- `dobj`: Pointer to the associated DumpableObject structure containing dump information, or NULL if not applicable
- `ext`: Pointer to the ExtensionInfo structure if this object is owned by an extension, or NULL otherwise

## Dependencies
- Types referenced:
  - CatalogId (from pg_backup.h)
  - DumpableObject (from pg_dump.h)
  - ExtensionInfo (from pg_dump.h)
- Used by functions:
  - AssignDumpId (src/bin/pg_dump/common.c:685)
  - recordAdditionalCatalogID (src/bin/pg_dump/common.c:710)
  - findObjectByCatalogId (src/bin/pg_dump/common.c:769)
  - recordExtensionMembership (src/bin/pg_dump/common.c:1054)
  - findOwningExtension (src/bin/pg_dump/common.c:1078)

## Notes and Other Information
- This structure is used as SH_ELEMENT_TYPE in a hash table implementation (referenced at src/bin/pg_dump/common.c:69)
- The hash table provides efficient O(1) average-case lookup performance for mapping catalog IDs to dump objects
- Extension membership tracking is crucial for pg_dump to properly handle extension-owned objects during dump and restore operations
- The structure supports both standalone database objects and extension-owned objects through the optional ext pointer