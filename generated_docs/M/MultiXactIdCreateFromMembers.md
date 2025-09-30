# MultiXactIdCreateFromMembers

## Location
[src/backend/access/transam/multixact.c:814-909](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/multixact.c#L814-L909)

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
  - [mXactCacheGetBySet](../m/mXactCacheGetBySet.md) (cache lookup)
  - MultiXactIdIsValid (validation)
  - ISUPDATE_from_mxstatus (status checking)
  - [GetNewMultiXactId](../G/GetNewMultiXactId.md) (ID assignment)
  - [XLogBeginInsert](../X/XLogBeginInsert.md), XLogRegisterData, XLogInsert (WAL logging)
  - [RecordNewMultiXact](../R/RecordNewMultiXact.md) (SLRU storage)
  - [mXactCachePut](../m/mXactCachePut.md) (caching)
  - debug_elog3, debug_elog2 (debugging)
- Called from (representative examples):
  - [FreezeMultiXactId](../F/FreezeMultiXactId.md) (during vacuum operations)
  - [MultiXactIdCreate](MultiXactIdCreate.md) (creating new MultiXacts)
  - [MultiXactIdExpand](MultiXactIdExpand.md) (expanding existing MultiXacts)

## Notes and Other Information
- The members array is sorted in-place during processing
- Uses cache optimization to avoid creating duplicate MultiXacts
- Validates that at most one member has update privileges
- Creates proper WAL entries for crash recovery
- Used in vacuum operations where the current backend may not be a member
- Critical sections ensure atomicity of the creation process
- The function handles both member creation and storage in SLRU files

## Simplified Source

```c
MultiXactId MultiXactIdCreateFromMembers(int nmembers, MultiXactMember *members) {
    MultiXactId multi;
    MultiXactOffset offset;

    // Check cache first - avoid creating duplicate MultiXacts
    multi = mXactCacheGetBySet(nmembers, members);
    if (MultiXactIdIsValid(multi)) {
        return multi;  // Found existing MultiXact with same members
    }

    // Validate: at most one updating member allowed
    bool has_update = false;
    for (int i = 0; i < nmembers; i++) {
        if (ISUPDATE_from_mxstatus(members[i].status)) {
            if (has_update) {
                elog(ERROR, "new multixact has more than one updating member");
            }
            has_update = true;
        }
    }

    // Assign new MultiXact ID and offset range (starts critical section)
    multi = GetNewMultiXactId(nmembers, &offset);

    // Create WAL record for crash recovery
    xl_multixact_create xlrec;
    xlrec.mid = multi;
    xlrec.moff = offset;
    xlrec.nmembers = nmembers;

    XLogBeginInsert();
    XLogRegisterData((char *) (&xlrec), SizeOfMultiXactCreate);
    XLogRegisterData((char *) members, nmembers * sizeof(MultiXactMember));
    XLogInsert(RM_MULTIXACT_ID, XLOG_MULTIXACT_CREATE_ID);

    // Store in SLRU files (OFFSETs and MEMBERs)
    RecordNewMultiXact(multi, offset, nmembers, members);

    END_CRIT_SECTION();

    // Cache the new MultiXact for future lookups
    mXactCachePut(multi, nmembers, members);

    return multi;
}
```