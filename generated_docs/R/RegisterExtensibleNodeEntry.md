# RegisterExtensibleNodeEntry

## Location
[src/backend/nodes/extensible.c:39-75](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/extensible.c#L39-L75)

## Overview
An internal function that registers a new callback structure in the extensible node system, creating and managing hash tables for extensible node type registration.

## Definition
```c
static void RegisterExtensibleNodeEntry(HTAB **p_htable, const char *htable_label,
                                       const char *extnodename,
                                       const void *extnodemethods)
```

## Detailed Description
This function serves as the core mechanism for registering extensible nodes in PostgreSQL. It manages the creation and population of hash tables that store extensible node type information and their associated method callbacks. The function ensures that each extensible node type is uniquely registered and prevents duplicate registrations. When called for the first time with a given hash table pointer, it creates the hash table with appropriate settings for string-based keys and ExtensibleNodeEntry values.

## Parameters / Member Variables
- `p_htable`: Pointer to hash table pointer that will store the extensible node entries
- `htable_label`: Descriptive label for the hash table used during creation
- `extnodename`: Name identifier for the extensible node type being registered
- `extnodemethods`: Pointer to the method structure containing callbacks for this node type

## Dependencies
- Functions called/Symbols referenced:
  - [hash_create](../h/hash_create.md)
  - [hash_search](../h/hash_search.md)
  - elog
  - ereport
  - strlen
- Data types used:
  - [HTAB](../H/HTAB.md)
  - ExtensibleNodeEntry
  - [HASHCTL](../H/HASHCTL.md)
  - EXTNODENAME_MAX_LEN
- Called from (representative examples):
  - [RegisterExtensibleNodeMethods](RegisterExtensibleNodeMethods.md)
  - [RegisterCustomScanMethods](RegisterCustomScanMethods.md)

## Notes and Other Information
- This is a static internal function, not exposed in the public API
- Performs validation to ensure extensible node names don't exceed EXTNODENAME_MAX_LEN
- Uses PostgreSQL's hash table infrastructure with HASH_ELEM and HASH_STRINGS options
- Throws ERROR if duplicate node type registration is attempted
- [Hash](../H/Hash.md) table is created lazily on first registration attempt
- Initial hash table capacity is set to 100 entries

## Simplified Source

```c
static void RegisterExtensibleNodeEntry(HTAB **p_htable, const char *htable_label,
                                       const char *extnodename,
                                       const void *extnodemethods) {
    ExtensibleNodeEntry *entry;
    bool found;

    // Create hash table if it doesn't exist yet
    if (*p_htable == NULL) {
        HASHCTL ctl;
        ctl.keysize = EXTNODENAME_MAX_LEN;
        ctl.entrysize = sizeof(ExtensibleNodeEntry);
        *p_htable = hash_create(htable_label, 100, &ctl, HASH_ELEM | HASH_STRINGS);
    }

    // Validate node name length
    if (strlen(extnodename) >= EXTNODENAME_MAX_LEN)
        elog(ERROR, "extensible node name is too long");

    // Add entry to hash table, checking for duplicates
    entry = (ExtensibleNodeEntry *) hash_search(*p_htable, extnodename, HASH_ENTER, &found);
    if (found)
        ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT),
                       errmsg("extensible node type \"%s\" already exists", extnodename)));

    // Store the method callbacks
    entry->extnodemethods = extnodemethods;
}
```