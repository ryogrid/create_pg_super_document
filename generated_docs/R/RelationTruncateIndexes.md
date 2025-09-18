# RelationTruncateIndexes

## Location
src/backend/catalog/heap.c: 3021 - 3068

## Overview
RelationTruncateIndexes truncates and rebuilds all indexes associated with a heap relation, effectively removing all index entries and reconstructing empty indexes.

## Definition
static void RelationTruncateIndexes(Relation heapRelation)

## Detailed Description
This static function is responsible for truncating all indexes associated with a heap relation when the heap itself is being truncated. It iterates through all indexes of the specified relation, opens each index with an exclusive lock, truncates the index file to zero blocks using RelationTruncate, and then rebuilds the index structure from scratch using index_build.

The function uses a dummy IndexInfo structure during rebuilding to avoid executing user-defined code in index expressions or predicates, which is important during operations like ON COMMIT processing. The caller must hold an exclusive lock on the heap relation before calling this function.

## Parameters / Member Variables
- : The heap relation whose associated indexes need to be truncated and rebuilt

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetIndexList
  - lfirst_oid
  - index_open
  - AccessExclusiveLock
  - BuildDummyIndexInfo
  - RelationTruncate
  - index_build
  - index_close
  - NoLock
- Called from (representative examples):
  - heap_truncate_one_rel

## Notes and Other Information
- This is a static function, only accessible within heap.c
- The function requires the caller to hold an exclusive lock on the heap relation
- Uses dummy IndexInfo to avoid running user-defined code during index rebuilding
- Each index is opened with AccessExclusiveLock and closed with NoLock to avoid deadlock issues
- The index rebuild process creates completely empty indexes ready for new data
- This function is typically called as part of TRUNCATE operations that need to maintain index structures