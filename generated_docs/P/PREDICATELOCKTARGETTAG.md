# PREDICATELOCKTARGETTAG

## Location
[src/include/storage/predicate_internals.h:267-273](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/predicate_internals.h#L267-L273)

## Overview
A structure that serves as a hash key to identify database objects (relations, pages, tuples) that can be targets of predicate locks in PostgreSQL's serializable snapshot isolation system.

## Definition

```c
typedef struct PREDICATELOCKTARGETTAG
{
	uint32		locktag_field1; /* a 32-bit ID field */
	uint32		locktag_field2; /* a 32-bit ID field */
	uint32		locktag_field3; /* a 32-bit ID field */
	uint32		locktag_field4; /* a 32-bit ID field */
} PREDICATELOCKTARGETTAG;
```
## Detailed Description
PREDICATELOCKTARGETTAG is a versatile identification structure used to uniquely identify database objects that can be targets of predicate locks in PostgreSQL's serializable snapshot isolation implementation. The structure consists of four generic 32-bit fields that can represent different combinations of database identifiers depending on the granularity of the lock target. For example, these fields might represent database OID, relation OID, block number, and tuple offset for tuple-level locks, or just database OID and relation OID for relation-level locks. The flexible design allows the same structure to identify locks at different granularities (relation, page, or tuple level). The structure is carefully designed to work with hash functions, though there are important considerations about field alignment and initialization to ensure proper hashing behavior.

## Parameters / Member Variables
- `locktag_field1`: A 32-bit ID field, typically used for database OID or other high-level identifier
- `locktag_field2`: A 32-bit ID field, typically used for relation OID or tablespace identifier
- `locktag_field3`: A 32-bit ID field, typically used for block number or additional relation identifier
- `locktag_field4`: A 32-bit ID field, typically used for tuple offset or other fine-grained identifier
## Dependencies
- Functions called/Symbols referenced:
  - uint32 (base integer type)
- Called from (representative examples):
  - [SerialControl](../S/SerialControl.md) (extensive usage in predicate locking control structure)
  - [InitPredicateLocks](../I/InitPredicateLocks.md) (predicate locking system initialization)
  - [GetPredicateLockStatusData](../G/GetPredicateLockStatusData.md) (lock status reporting)
  - [CreateLocalPredicateLockHash](../C/CreateLocalPredicateLockHash.md) (local predicate lock hash creation)
  - [PageIsPredicateLocked](PageIsPredicateLocked.md) (page lock checking)
  - [PredicateLockExists](PredicateLockExists.md) (lock existence checking)
  - [GetParentPredicateLockTag](../G/GetParentPredicateLockTag.md) (parent lock tag construction)
  - [CoarserLockCovers](../C/CoarserLockCovers.md) (lock coverage checking)
  - [PredicateLockRelation](PredicateLockRelation.md) (relation-level predicate locking)
  - [PredicateLockPage](PredicateLockPage.md) (page-level predicate locking)
  - [PredicateLockTID](PredicateLockTID.md) (tuple-level predicate locking)
  - [PREDICATELOCKTARGET](PREDICATELOCKTARGET.md) (uses this as tag field)
  - [LOCALPREDICATELOCK](../L/LOCALPREDICATELOCK.md) (uses this as tag field)

## Notes and Other Information
- The generic field design allows the same structure to identify different lock granularities
- [Hash](../H/Hash.md) function considerations require careful initialization of any slack space in the structure
- Structure size must be carefully managed to ensure proper hash function behavior
- Currently under consideration for field renaming to make usage patterns clearer
- Essential component of the predicate locking system that prevents serialization anomalies
- Used extensively throughout the predicate locking subsystem for lock identification and management
- Supports both local and shared predicate lock hash tables
- Critical for implementing granularity escalation in predicate locking (tuple → page → relation)