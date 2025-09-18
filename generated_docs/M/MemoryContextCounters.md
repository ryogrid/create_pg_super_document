# MemoryContextCounters

## Location
[src/include/nodes/memnodes.h:29-35](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/memnodes.h#L29-L35)

## Overview
A structure that provides summarization state for memory context statistics collection, primarily designed to track allocation patterns and memory usage in PostgreSQL's memory management system.

## Definition


## Detailed Description
MemoryContextCounters is a statistics aggregation structure used throughout PostgreSQL's memory management system to collect and summarize memory usage information. The design is biased towards AllocSet memory contexts, which are the most commonly used context type in PostgreSQL. The structure provides a standardized way to report memory statistics in the format historically used by AllocSet contexts.

The counters track both allocated and free memory at the block and chunk level, allowing for detailed analysis of memory usage patterns. This information is crucial for performance monitoring, debugging memory issues, and understanding allocation behavior in PostgreSQL.

## Parameters / Member Variables
- : Total number of malloc blocks allocated by the memory context
- : Total number of free chunks available for reuse within allocated blocks  
- : Total bytes requested from the underlying malloc implementation
- : The amount of unused space within the total allocated space

## Dependencies
- Functions called/Symbols referenced:
  - Size (PostgreSQL's size type)
- Called from (representative examples):
  - [show_memory_counters](../s/show_memory_counters.md) (explain.c:3950)
  - [AllocSetStats](../A/AllocSetStats.md) (aset.c:1523)
  - [BumpStats](../B/BumpStats.md) (bump.c:689)
  - [GenerationStats](../G/GenerationStats.md) (generation.c:1035)
  - [SlabStats](../S/SlabStats.md) (slab.c:931)
  - [MemoryContextStatsDetail](MemoryContextStatsDetail.md) (mcxt.c:833)

## Notes and Other Information
- The structure is designed with AllocSet contexts in mind, but is used by all memory context types
- Future memory context implementations with fundamentally different approaches may require additional or different counters
- The API design allows for printing only nonzero counters in some contexts
- Used extensively in EXPLAIN command output and backend memory context reporting functions
- Located in src/include/nodes/memnodes.h, making it available throughout the PostgreSQL codebase