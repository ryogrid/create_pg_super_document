# BlockRefTableKey

## Location
src/common/blkreftable.c: 47 - 51

## Overview
BlockRefTableKey is a structure that serves as a unique identifier for tracking the status of each relation fork individually within PostgreSQL's block reference table system.

## Definition

```c
typedef struct BlockRefTableKey
{
	RelFileLocator rlocator;
	ForkNumber	forknum;
} BlockRefTableKey;
```
## Detailed Description
BlockRefTableKey is a composite key structure used by the block reference table to uniquely identify and track different forks of database relations. The structure combines a relation file locator with a fork number to create a unique key that can distinguish between different types of forks (main, FSM, VM, etc.) of the same relation. This allows the block reference table to maintain separate tracking for each fork's block status independently.

## Parameters / Member Variables
- : RelFileLocator that identifies the specific relation file, containing database, tablespace, and relation OID information
- : ForkNumber that specifies which fork of the relation this key refers to (e.g., MAIN_FORKNUM, FSM_FORKNUM, VISIBILITYMAP_FORKNUM)

## Dependencies
- Functions called/Symbols referenced: None (structure definition only)
- Used by:
  - BlockRefTableEntry (as part of hash table entry structure)
  - SH_KEY_TYPE (hash table key type definition)
  - SH_HASH_KEY (hash function key parameter)
  - SH_EQUAL (equality comparison function)
  - BlockRefTableSetLimitBlock
  - BlockRefTableMarkBlockModified
  - BlockRefTableGetEntry
  - WriteBlockRefTable

## Notes and Other Information
- This structure is defined in src/common/blkreftable.c:47-51
- Serves as the hash table key for PostgreSQL's block reference table implementation
- The combination of RelFileLocator and ForkNumber ensures unique identification across all relation forks in the system
- Essential for maintaining separate block modification tracking for different fork types of the same relation