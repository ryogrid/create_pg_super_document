# MemoryContextStats

## Location
[src/backend/utils/mmgr/mcxt.c:814-828](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/mcxt.c#L814-L828)

## Overview
MemoryContextStats is a debugging utility function that prints comprehensive statistics about a specified memory context and all its descendant contexts to stderr.

## Definition
```c
void MemoryContextStats(MemoryContext context)
```

## Detailed Description
This function serves as a convenient wrapper around MemoryContextStatsDetail, providing a simple interface for obtaining memory context statistics during debugging sessions. It uses hard-coded reasonable limits for output formatting and automatically includes summary information when the output would otherwise be very long. The function is designed primarily for debugging purposes and outputs all statistics to stderr for immediate visibility during development and troubleshooting.

## Parameters / Member Variables
- `context`: The memory context for which to print statistics, including all its descendant contexts in the context hierarchy

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextStatsDetail](MemoryContextStatsDetail.md)
- Called from (representative examples):
  - [finish_xact_command](../f/finish_xact_command.md) (transaction cleanup debugging)
  - [MemoryContextAllocationFailure](MemoryContextAllocationFailure.md) (error reporting)
  - [AllocSetContextCreateInternal](../A/AllocSetContextCreateInternal.md) (context creation debugging)
  - Various test functions for memory validation

## Notes and Other Information
- This is a debugging-only utility function and should not be used in production code paths
- Uses hard-wired limits (100, 100, true) for max_children, max_total_children, and include_details parameters
- Output is directed to stderr to avoid interfering with normal program output
- The function makes efforts to summarize output when dealing with large context hierarchies to keep the output manageable
- Commonly used during memory leak investigation and context hierarchy analysis