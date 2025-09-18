# GetExtensibleNodeEntry

## Location
[src/backend/nodes/extensible.c:100-124](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/extensible.c#L100-L124)

## Overview
An internal routine that retrieves an ExtensibleNodeEntry by its identifier from the specified hash table, with optional error handling.

## Definition
```c
static const void *GetExtensibleNodeEntry(HTAB *htable, const char *extnodename, bool missing_ok)
```

## Detailed Description
This function serves as the core lookup mechanism for extensible node entries in PostgreSQL's extensible node system. It searches the provided hash table for an entry matching the given extensible node name and returns the associated method structure. The function supports both strict and lenient lookup modes: when missing_ok is false, it raises an ERROR if the node type is not found; when missing_ok is true, it returns NULL for missing entries, allowing callers to handle the absence gracefully.

## Parameters / Member Variables
- `htable`: Hash table to search in (can be NULL, which results in immediate miss)
- `extnodename`: Name identifier of the extensible node type to look up
- `missing_ok`: If true, returns NULL for missing entries; if false, raises ERROR for missing entries

## Dependencies
- Functions called/Symbols referenced:
  - [hash_search](../h/hash_search.md)
  - ereport
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
- Data types used:
  - [HTAB](../H/HTAB.md)
  - ExtensibleNodeEntry
  - HASH_FIND
- Called from (representative examples):
  - [GetExtensibleNodeMethods](GetExtensibleNodeMethods.md)
  - [GetCustomScanMethods](GetCustomScanMethods.md)

## Notes and Other Information
- This is a static internal function, not exposed in the public API
- Returns a pointer to the method structure, not the ExtensibleNodeEntry itself
- Handles NULL hash table gracefully by treating it as a lookup miss
- Error message specifically mentions 'ExtensibleNodeMethods' in the error text
- Uses ERRCODE_UNDEFINED_OBJECT for missing node type errors
- The function is generic and works with any hash table containing ExtensibleNodeEntry structures