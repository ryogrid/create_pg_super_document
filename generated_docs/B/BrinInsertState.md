# BrinInsertState

## Location
src/backend/access/brin/brin.c: 189 - 194

## Overview
BrinInsertState captures running state that spans multiple brininsert invocations within the same command, providing efficient reuse of index access structures.

## Definition


## Detailed Description
BrinInsertState is a lightweight state structure designed to optimize multiple insert operations within a single command by caching essential access structures. Rather than reinitializing the reverse map access and index descriptor for each insert operation, this state allows them to be reused across multiple brininsert calls, improving performance for bulk insert operations.

The structure is particularly important for maintaining efficiency when inserting many tuples in a single transaction or statement, as it avoids the overhead of repeatedly setting up the same access structures.

## Parameters / Member Variables
- : Pointer to the BRIN reverse map access structure for efficient range lookup and maintenance
- : Pointer to the BRIN index descriptor containing operator class information and index metadata
- : Number of heap pages covered by each BRIN range in this index

## Dependencies
- Functions called/Symbols referenced:
  - [BrinRevmap](BrinRevmap.md)
  - [BrinDesc](BrinDesc.md)
- Called from (representative examples):
  - [brinhandler](../b/brinhandler.md)
  - [initialize_brin_insertstate](../i/initialize_brin_insertstate.md)
  - [brininsert](../b/brininsert.md)
  - [brininsertcleanup](../b/brininsertcleanup.md)

## Notes and Other Information
This state structure is specifically designed for insert operations and is much simpler than BrinBuildState since it doesn't need to track build progress or coordinate parallel operations. The state is typically initialized once per command and reused for multiple insert operations, then cleaned up when the command completes. The bis_pages_per_range value is cached to avoid repeated lookups during insert operations.