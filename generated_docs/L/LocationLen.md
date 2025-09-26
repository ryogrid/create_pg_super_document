# LocationLen

## Location
[src/include/nodes/queryjumble.h:22-26](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/queryjumble.h#L22-L26)

## Overview
LocationLen is a structure used for tracking the locations and lengths of constants during query normalization and fingerprinting processes.

## Definition

```c
typedef struct LocationLen
{
	int			location;		/* start offset in query text */
	int			length;			/* length in bytes, or -1 to ignore */
} LocationLen;
```
## Detailed Description
LocationLen serves as a fundamental data structure in PostgreSQL's query jumbling and normalization system. It tracks the position and size of constants within query text that need to be replaced or normalized during the query fingerprinting process. This structure is essential for creating normalized query representations where constants are abstracted away, allowing similar queries with different constant values to have the same fingerprint.

The structure is used to build an array of constant locations that can later be processed to generate normalized query text where constants are replaced with placeholders.

## Parameters / Member Variables
- `location`: The start offset (byte position) in the original query text where a constant begins. A value of -1 indicates an unknown or undefined location.
- `length`: The length of the constant in bytes. A value of -1 is used to indicate that the length should be ignored, which simplifies usage for third-party modules.
## Dependencies
- Functions called/Symbols referenced:
  - Used within JumbleState structure
  - Allocated using palloc/repalloc functions
- Called from (representative examples):
  - [RecordConstLocation](../R/RecordConstLocation.md) function in queryjumblefuncs.c:212-214
  - [JumbleState](../J/JumbleState.md).clocations array allocation in queryjumblefuncs.c:117-118

## Notes and Other Information
- This structure is primarily used as elements in the clocations array within JumbleState
- The length field is initialized to -1 by default to simplify third-party module usage
- Part of PostgreSQL's query normalization system that enables efficient query pattern matching
- Essential for features like pg_stat_statements that need to group similar queries together
- Located in src/include/nodes/queryjumble.h:22-26