# RecordConstLocation

## Location
[src/backend/nodes/queryjumblefuncs.c:198-218](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/queryjumblefuncs.c#L198-L218)

## Overview
RecordConstLocation tracks the source code locations of constants within SQL query strings during the query jumbling process for later parameter extraction.

## Definition

```c
static void
RecordConstLocation(JumbleState *jstate, int location)
```
## Detailed Description
RecordConstLocation maintains a record of where constants appear in the original SQL query text during the query jumbling process. This location information is crucial for third-party modules (like pg_stat_statements) that need to extract and replace constant values with parameter placeholders for query normalization. The function manages a dynamically-growing array of LocationLen structures, doubling the buffer size when more space is needed. Location values of -1 are ignored as they indicate unknown or undefined positions.

## Parameters / Member Variables
- : JumbleState containing the constant locations buffer and metadata
- : Position of the constant in the original query string (-1 for unknown/undefined locations)

## Dependencies
- Functions called/Symbols referenced:
  - [repalloc](../r/repalloc.md) (reallocates memory when buffer needs expansion)
  - [LocationLen](../L/LocationLen.md) (structure type for storing location and length information)
  - [JumbleState](../J/JumbleState.md) (state structure containing location tracking arrays)
- Called from (representative examples):
  - JUMBLE_LOCATION (macro for recording locations during jumbling)

## Notes and Other Information
- Static function (internal to queryjumblefuncs.c)
- Implements dynamic buffer management with doubling growth strategy
- Initializes length field to -1 to simplify usage by third-party modules
- Ignores negative location values which indicate unknown positions
- Essential for parameter extraction and query normalization features
- Used in conjunction with constant value jumbling to maintain source location mapping
- Part of the infrastructure that enables pg_stat_statements to show parameterized queries
- Buffer starts with initial size of 32 locations and grows as needed