# HashScanPosItem

## Location
src/include/access/hash.h: 103 - 107

## Overview
HashScanPosItem is a structure that stores information about individual matched items during hash index scans, containing both heap and index location data.

## Definition


## Detailed Description
HashScanPosItem represents a single match found during a hash index scan operation. This structure is essential for maintaining the relationship between index entries and their corresponding heap tuples, while also tracking the precise location of index items within pages. This dual tracking capability is crucial for operations that need to revisit or modify index entries, such as killing dead tuples or handling concurrent updates.

The structure supports PostgreSQL's MVCC (Multi-Version Concurrency Control) system by maintaining precise location information that allows the system to efficiently locate and process index entries during various scan operations.

## Parameters / Member Variables
- : Transaction identifier (TID) pointing to the corresponding tuple in the heap table
- : The offset number indicating the specific location of the index item within its page

## Dependencies
- Functions called/Symbols referenced:
  - [ItemPointerData](../I/ItemPointerData.md)
  - OffsetNumber
- Called from (representative examples):
  - [hashgetbitmap](../h/hashgetbitmap.md)
  - [_hash_next](../h/_hash_next.md)
  - [_hash_first](../h/_hash_first.md)
  - [_hash_saveitem](../h/_hash_saveitem.md)
  - [_hash_kill_items](../h/_hash_kill_items.md)
  - [HashScanPosData](HashScanPosData.md)

## Notes and Other Information
This structure is typically used as part of larger scan state management structures, particularly in HashScanPosData arrays. The separation of heap TID and index offset allows for efficient tuple identification while maintaining the ability to perform page-level operations on the index itself. This design is particularly important for handling dead tuple cleanup and maintaining index consistency during concurrent operations.