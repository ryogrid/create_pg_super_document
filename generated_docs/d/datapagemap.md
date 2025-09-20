# datapagemap

## Location
[src/bin/pg_rewind/datapagemap.h:15-20](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_rewind/datapagemap.h#L15-L20)

## Overview
A structure used by pg_rewind to represent a bitmap of data pages that need to be synchronized between PostgreSQL clusters.

## Definition

```c
struct datapagemap
{
	char	   *bitmap;
	int			bitmapsize;
};
```
## Detailed Description
The datapagemap structure is a core component of PostgreSQL's pg_rewind utility, which is used to resynchronize a PostgreSQL cluster with another cluster. This structure maintains a bitmap that tracks which data pages need to be copied or synchronized during the rewind process. Each bit in the bitmap corresponds to a specific block number, allowing efficient tracking of pages that have been modified and require attention during the rewind operation.

## Parameters / Member Variables
- : A character array that serves as the actual bitmap storage, where each bit represents the status of a corresponding data page block
- : An integer that stores the size of the bitmap in bytes, used for memory management and boundary checking

## Dependencies
- Functions called/Symbols referenced:
  - (No direct references from this symbol)
- Called from (representative examples):
  - [datapagemap_t](datapagemap_t.md) (used as base type for typedef)

## Notes and Other Information
- This structure is fundamental to pg_rewind's page-level tracking mechanism
- The bitmap allows for efficient memory usage compared to maintaining a list of individual block numbers
- Located in src/bin/pg_rewind/datapagemap.h:15-20
- Used as the underlying structure for the datapagemap_t typedef