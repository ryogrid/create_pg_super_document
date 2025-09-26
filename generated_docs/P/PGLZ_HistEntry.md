# PGLZ_HistEntry

## Location
[src/common/pg_lzcompress.c:210-216](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/pg_lzcompress.c#L210-L216)

## Overview
A structure used in PostgreSQL's PGLZ compression algorithm to maintain a doubly-linked list of history entries for backward reference lookup during LZ77-style compression.

## Definition

```c
typedef struct PGLZ_HistEntry
{
	struct PGLZ_HistEntry *next;	/* links for my hash key's list */
	struct PGLZ_HistEntry *prev;
	int			hindex;			/* my current hash key */
	const char *pos;			/* my input position */
} PGLZ_HistEntry;
```
## Detailed Description
PGLZ_HistEntry is a fundamental data structure in PostgreSQL's PGLZ (PostgreSQL LZ) compression implementation, which uses a variant of the LZ77 compression algorithm. This structure represents an entry in the compression history table that tracks previously seen byte sequences in the input data.

The structure implements a doubly-linked list where all entries sharing the same hash key are linked together. This design allows for efficient removal of entries when they become too old (more than 4K positions behind the current position) and need to be recycled. The hash-based organization enables fast lookup of potential matches during compression.

Each history entry points to a specific position in the input buffer and is indexed by a hash value computed from the bytes at that position. When the compressor encounters new data, it searches through the linked list of entries with matching hash values to find the longest matching sequence, which can then be encoded as a back-reference instead of literal bytes.

## Parameters / Member Variables
- `*next`: Pointer to the next PGLZ_HistEntry in the doubly-linked list for entries sharing the same hash key
- `*prev`: Pointer to the previous PGLZ_HistEntry in the doubly-linked list, used for efficient removal during recycling
- `hindex`: The hash key/index value for this entry, computed from the byte sequence at the referenced position
- `*pos`: Pointer to the position in the input buffer that this history entry represents
## Dependencies
- Functions that use this structure:
  - pglz_hist_add (macro that adds new entries to the history table)
  - [pglz_find_match](../p/pglz_find_match.md) (function that searches for matching sequences using the history entries)
- Related constants:
  - PGLZ_HISTORY_SIZE (4096) - maximum number of history entries
  - PGLZ_MAX_MATCH (273) - maximum length of a match sequence
  - PGLZ_MAX_HISTORY_LISTS (8192) - number of hash buckets for the history table

## Notes and Other Information
- The structure is used within a circular buffer of PGLZ_HISTORY_SIZE entries (4096 entries)
- Entries are recycled in a round-robin fashion when the history buffer becomes full
- The 0th entry in the history table is intentionally unused to simplify pointer arithmetic and serve as a sentinel value
- The hash-based organization allows for O(1) average-case lookup time for finding potential matches
- This compression method is used for TOAST (The Oversized-Attribute Storage Technique) data compression and WAL (Write-Ahead Log) compression in PostgreSQL
- The doubly-linked list design enables efficient removal of stale entries without needing to traverse the entire chain