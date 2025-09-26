# TLHistoryFileName

## Location
[src/include/access/xlog_internal.h:218-223](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/xlog_internal.h#L218-L223)

## Overview
TLHistoryFileName is an inline function that generates the filename for a timeline history file based on a given timeline ID, following PostgreSQL's standard naming convention for timeline history files.

## Definition

```c
static inline void
TLHistoryFileName(char *fname, TimeLineID tli)
```
## Detailed Description
This function constructs a filename for a timeline history file by formatting the timeline ID into PostgreSQL's standard timeline history file naming convention. Timeline history files store the branching history of WAL timelines and are named using the format "TTTTTTTT.history" where TTTTTTTT is the 8-digit hexadecimal representation of the timeline ID. These files are crucial for understanding the relationship between different timeline branches during point-in-time recovery and replication scenarios.

## Parameters / Member Variables
- : Output buffer that receives the constructed filename (must be at least MAXFNAMELEN bytes)
- : Timeline ID for which to generate the history filename

## Dependencies
- Functions called/Symbols referenced:
  - MAXFNAMELEN (maximum filename length constant)
- Called from (representative examples):
  - readTimeLineHistory (reads timeline history from file)
  - writeTimeLineHistory (writes timeline history to file) 
  - existsTimeLineHistory (checks if timeline history file exists)
  - restoreTimeLineHistoryFiles (restores timeline history during recovery)
  - SendTimeLineHistory (sends timeline history during replication)

## Notes and Other Information
- This is an inline function defined in the header for performance optimization
- Timeline history files are essential for WAL replay and replication consistency
- The .history extension is a standard PostgreSQL convention for timeline metadata files
- Timeline IDs start from 1, with higher numbers representing newer timeline branches
- History files contain information about when timeline switches occurred and their parent timelines
- These files are replicated along with WAL data to ensure consistency across standby servers