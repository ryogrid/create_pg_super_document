# mdopen

## Location
[src/backend/storage/smgr/md.c:680-690](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/smgr/md.c#L680-L690)

## Overview
mdopen initializes a newly-opened relation by resetting the segment counters for all fork types to mark the relation as not having any open segments.

## Definition
```c
void mdopen(SMgrRelation reln)
```

## Detailed Description
The mdopen function is part of PostgreSQL's magnetic disk (md) storage manager interface. It performs initialization tasks when a relation is first opened through the storage manager. The primary responsibility is to reset the md_num_open_segs array for all fork types (main, FSM, VM, etc.) to zero, effectively marking that no segments of this relation are currently open at the md layer.

This function is called as part of the storage manager's relation opening process and ensures that the md-specific state is properly initialized before any I/O operations are performed on the relation.

## Parameters / Member Variables
- `reln`: SMgrRelation pointer representing the relation being opened. Contains metadata and state information for the relation across all storage manager layers.

## Dependencies
- Functions called/Symbols referenced:
  - MAX_FORKNUM (constant defining maximum fork number)
- Called from (representative examples):
  - Referenced in MD_H header file for external access

## Notes and Other Information
- This function operates on all fork types from 0 to MAX_FORKNUM, ensuring consistent initialization across main data, free space map, visibility map, and any other fork types
- The md_num_open_segs array tracks how many segments are currently open for each fork of the relation
- This is a lightweight initialization function that only modifies the segment counter state without performing any disk I/O operations
- Part of PostgreSQL's pluggable storage manager architecture where md.c implements the traditional heap storage method

## Simplified Source

```c
void mdopen(SMgrRelation reln)
{
    // Initialize all fork segment counters to zero (mark as not open)
    for (int forknum = 0; forknum <= MAX_FORKNUM; forknum++)
        reln->md_num_open_segs[forknum] = 0;
}
```

**Key Points:**
- Initializes a newly-opened relation in the magnetic disk storage manager
- Resets segment counters for all fork types (main, FSM, VM, etc.) to zero
- Marks that no segments are currently open for this relation
- Lightweight initialization with no disk I/O, only state management
- Part of PostgreSQL's storage manager initialization sequence