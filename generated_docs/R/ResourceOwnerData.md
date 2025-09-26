# ResourceOwnerData

## Location
src/backend/utils/resowner/resowner.c: 110 - 179

## Overview
ResourceOwnerData is the main data structure that implements PostgreSQL's resource ownership tracking system, managing hierarchical resource ownership with efficient storage and cleanup mechanisms.

## Definition
```c
struct ResourceOwnerData
{
    ResourceOwner parent;        /* NULL if no parent (toplevel owner) */
    ResourceOwner firstchild;    /* head of linked list of children */
    ResourceOwner nextchild;     /* next child of same parent */
    const char *name;            /* name (just for debugging) */

    bool        releasing;
    bool        sorted;          /* are 'hash' and 'arr' sorted by priority? */

    uint8       nlocks;          /* number of owned locks */
    uint8       narr;            /* how many items are stored in the array */
    uint32      nhash;           /* how many items are stored in the hash */

    ResourceElem arr[RESOWNER_ARRAY_SIZE];

    ResourceElem *hash;
    uint32      capacity;        /* allocated length of hash[] */
    uint32      grow_at;         /* grow hash when reach this */

    LOCALLOCK  *locks[MAX_RESOWNER_LOCKS];  /* list of owned locks */
};
```

## Detailed Description
ResourceOwnerData implements a sophisticated resource tracking system with hierarchical ownership and efficient storage mechanisms. The structure uses multiple storage strategies: a fixed-size array for recent resources (32 elements), a dynamically-sized hash table for overflow, and a specialized cache for locks (15 slots).

The hierarchy is maintained through parent-child relationships, enabling nested resource ownership contexts typical in PostgreSQL operations like transactions and subtransactions. During resource release, the system sorts resources by priority to ensure proper cleanup ordering, with the `releasing` and `sorted` flags coordinating the release process.

The hash table uses open addressing and grows dynamically when utilization reaches the `grow_at` threshold. When sorting is required for release, the hash table temporarily converts from hashed to sorted storage, with occupied elements moved to the front.

## Parameters / Member Variables
- `parent`: Pointer to parent ResourceOwner, NULL for top-level owners
- `firstchild`: Head of linked list connecting child ResourceOwners
- `nextchild`: Next sibling in the parent's child list
- `name`: Debug-only string identifier for the resource owner
- `releasing`: Flag indicating resource release is in progress
- `sorted`: Flag indicating arrays are sorted by release priority
- `nlocks`: Count of locks stored in the locks cache
- `narr`: Count of resources stored in the fixed array
- `nhash`: Count of resources stored in the hash table
- `arr[RESOWNER_ARRAY_SIZE]`: Fixed array for recently added resources (32 elements)
- `hash`: Dynamically allocated hash table for resource overflow
- `capacity`: Total allocated slots in the hash table
- `grow_at`: Threshold for hash table expansion
- `locks[MAX_RESOWNER_LOCKS]`: Specialized cache for LOCALLOCK pointers (15 slots)

## Dependencies
- Functions called/Symbols referenced:
  - ResourceOwner (typedef for ResourceOwnerData*)
  - ResourceElem (resource storage elements)
  - LOCALLOCK (lock management structure)
  - RESOWNER_ARRAY_SIZE (constant: 32)
  - MAX_RESOWNER_LOCKS (constant: 15)
- Called from (representative examples):
  - ResourceOwnerCreate (for creating new instances)
  - LOCALLOCKOWNER (as part of lock ownership tracking)

## Notes and Other Information
- The dual storage approach (array + hash table) optimizes for common cases where few resources are held while supporting scenarios with many resources
- The locks cache provides fast access to frequently-used lock resources, critical for transaction performance
- Priority-based sorting during release ensures resources are cleaned up in the correct order, preventing dependency issues
- The open-addressing hash table design minimizes memory fragmentation and provides good cache locality
- Field packing (nlocks, narr as uint8) optimizes struct layout to reduce memory overhead