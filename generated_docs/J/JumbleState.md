# JumbleState

## Location
[src/include/nodes/queryjumble.h:32-51](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/queryjumble.h#L32-L51)

## Overview
JumbleState is a working state structure used for computing query jumbles (fingerprints) and producing normalized query strings during PostgreSQL's query analysis and fingerprinting process.

## Definition

```c
typedef struct JumbleState
{
	/* Jumble of current query tree */
	unsigned char *jumble;

	/* Number of bytes used in jumble[] */
	Size		jumble_len;

	/* Array of locations of constants that should be removed */
	LocationLen *clocations;

	/* Allocated length of clocations array */
	int			clocations_buf_size;

	/* Current number of valid entries in clocations array */
	int			clocations_count;

	/* highest Param id we've seen, in order to start normalization correctly */
	int			highest_extern_param_id;
} JumbleState;
```
## Detailed Description
JumbleState serves as the central working data structure for PostgreSQL's query fingerprinting system. It accumulates a binary representation (jumble) of a query's structure while tracking the locations of constants that need normalization. The jumble is used to compute a unique query ID that allows similar queries with different constants to be grouped together.

The structure supports both the creation of query fingerprints and the generation of normalized query text by maintaining detailed location information about constants in the original query. This enables features like pg_stat_statements to efficiently aggregate statistics for similar query patterns.

## Parameters / Member Variables
- : A binary buffer (allocated as 1024 bytes initially) that accumulates the structural representation of the query tree as it's traversed
- : The current number of bytes used in the jumble buffer, tracking how much of the buffer contains valid data
- : A dynamically allocated array of LocationLen structures that records the positions and lengths of constants in the original query text
- : The allocated size of the clocations array, which grows as needed (starts at 32 entries)
- : The current number of valid entries stored in the clocations array
- : Tracks the highest external parameter ID encountered during jumbling, used for proper normalization of parameterized queries

## Dependencies
- Functions called/Symbols referenced:
  - [LocationLen](../L/LocationLen.md) (component structure)
  - [palloc](../p/palloc.md)/repalloc (memory allocation)
  - [hash_any_extended](../h/hash_any_extended.md) (for computing final query ID)
- Called from (representative examples):
  - [JumbleQuery](JumbleQuery.md) function in queryjumblefuncs.c:104-139 (main entry point)
  - [AppendJumble](../A/AppendJumble.md) function in queryjumblefuncs.c:161
  - [RecordConstLocation](../R/RecordConstLocation.md) function in queryjumblefuncs.c:198
  - [_jumbleNode](../j/_jumbleNode.md) and related jumbling functions

## Notes and Other Information
- The jumble buffer is fixed at JUMBLE_SIZE (1024 bytes) and uses hash chaining when content exceeds this size
- The clocations array starts with 32 entries and doubles in size when needed
- Essential for PostgreSQL's query normalization used by extensions like pg_stat_statements
- The structure is allocated and populated during query analysis when query ID computation is enabled
- Located in src/include/nodes/queryjumble.h:32-51