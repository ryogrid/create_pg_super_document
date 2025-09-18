# RegisterExtensibleNodeEntry

## Location
src/backend/nodes/extensible.c: 39 - 75

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
  - hash_create
  - hash_search
  - elog
  - ereport
  - strlen
- Data types used:
  - HTAB
  - ExtensibleNodeEntry
  - HASHCTL
  - EXTNODENAME_MAX_LEN
- Called from (representative examples):
  - RegisterExtensibleNodeMethods
  - RegisterCustomScanMethods

## Notes and Other Information
- This is a static internal function, not exposed in the public API
- Performs validation to ensure extensible node names don't exceed EXTNODENAME_MAX_LEN
- Uses PostgreSQL's hash table infrastructure with HASH_ELEM and HASH_STRINGS options
- Throws ERROR if duplicate node type registration is attempted
- Hash table is created lazily on first registration attempt
- Initial hash table capacity is set to 100 entries