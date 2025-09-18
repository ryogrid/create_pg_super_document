# RewriteMappingDataEntry

## Location
[src/backend/access/heap/rewriteheap.c:204-209](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/rewriteheap.c#L204-L209)

## Overview
RewriteMappingDataEntry represents a single in-memory logical rewrite mapping entry that links old and new tuple locations during heap rewrite operations, managed as part of a doubly-linked list.

## Definition
```c
typedef struct RewriteMappingDataEntry
{
    LogicalRewriteMappingData map;  /* map between old and new location of the tuple */
    dlist_node                node;
} RewriteMappingDataEntry;
```

## Detailed Description
RewriteMappingDataEntry serves as a container for individual logical rewrite mappings during PostgreSQL heap rewrite operations. Each entry contains the actual mapping data that tracks how tuple locations change from old to new storage locations, along with list management functionality. These entries are organized in doubly-linked lists hanging off RewriteMappingFile structures, allowing efficient traversal and manipulation of mapping collections. The structure bridges the gap between the persistent on-disk mapping format (LogicalRewriteMappingData) and the in-memory list management required during active rewrite operations.

## Parameters / Member Variables
- `map`: LogicalRewriteMappingData structure containing the actual mapping between old and new tuple locations, including relation file locators and item pointers for both old and new positions
- `node`: dlist_node structure providing doubly-linked list connectivity for efficient list operations and traversal

## Dependencies
- Functions called/Symbols referenced:
  - [LogicalRewriteMappingData](../L/LogicalRewriteMappingData.md)
  - [dlist_node](../d/dlist_node.md)
- Called from (representative examples):
  - [logical_heap_rewrite_flush_mappings](../l/logical_heap_rewrite_flush_mappings.md)
  - [logical_rewrite_log_mapping](../l/logical_rewrite_log_mapping.md)

## Notes and Other Information
This structure is specifically designed for logical replication scenarios where maintaining precise tuple location mappings is critical for consistency. The embedded LogicalRewriteMappingData contains the essential mapping information (old/new relation file locators and tuple IDs) while the dlist_node enables efficient list management during batch processing operations. The structure allows for both memory-efficient storage and fast sequential access patterns commonly needed during mapping file operations.