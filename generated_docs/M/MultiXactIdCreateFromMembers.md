# MultiXactIdCreateFromMembers

## Location
src/backend/access/transam/multixact.c: 814 - 909

## Overview
Creates a new MultiXactId from a specified set of transaction members, handling XLOG entries, SLRU storage, and caching for the new MultiXact.

## Definition
```c
MultiXactId MultiXactIdCreateFromMembers(int nmembers, MultiXactMember *members)
```

## Detailed Description
MultiXactIdCreateFromMembers is the core function for creating new MultiXact IDs from a set of transaction members. It performs several critical operations: first, it checks if an identical set of members already exists in the cache to avoid creating duplicates. It validates that there is at most one updating member among the given members. The function then assigns a new MultiXact ID and offset range, creates WAL (XLOG) entries for crash recovery, records the new MultiXact in SLRU storage, and caches the result for future lookups.

The function is designed to be crash-safe through proper WAL logging and uses critical sections to ensure atomicity. It is used in various contexts including vacuum operations and when expanding existing MultiXacts.

## Parameters / Member Variables
- `nmembers`: Number of transaction members in the MultiXact
- `members`: Array of MultiXactMember structures containing transaction IDs and their lock modes (will be sorted in-place)

## Dependencies
- Functions called/Symbols referenced:
  - mXactCacheGetBySet (cache lookup)
  - MultiXactIdIsValid (validation)
  - ISUPDATE_from_mxstatus (status checking)
  - GetNewMultiXactId (ID assignment)
  - XLogBeginInsert, XLogRegisterData, XLogInsert (WAL logging)
  - RecordNewMultiXact (SLRU storage)
  - mXactCachePut (caching)
  - debug_elog3, debug_elog2 (debugging)
- Called from (representative examples):
  - FreezeMultiXactId (during vacuum operations)
  - MultiXactIdCreate (creating new MultiXacts)
  - MultiXactIdExpand (expanding existing MultiXacts)

## Notes and Other Information
- The members array is sorted in-place during processing
- Uses cache optimization to avoid creating duplicate MultiXacts
- Validates that at most one member has update privileges
- Creates proper WAL entries for crash recovery
- Used in vacuum operations where the current backend may not be a member
- Critical sections ensure atomicity of the creation process
- The function handles both member creation and storage in SLRU files