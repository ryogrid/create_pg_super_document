# ginFinishOldSplit

## Location
src/backend/access/gin/ginbtree.c: 779 - 815

## Overview
ginFinishOldSplit completes previously incomplete page splits discovered during GIN B-tree traversal, handling lock upgrades safely for scenarios where incomplete splits are found opportunistically.

## Definition
```c
static void ginFinishOldSplit(GinBtree btree, GinBtreeStack *stack, 
                             GinStatsData *buildStats, int access)
```

## Detailed Description
ginFinishOldSplit serves as an entry point to ginFinishSplit for handling incomplete splits that were not created by the current operation but discovered during tree traversal. The key distinction from direct ginFinishSplit usage is the lock management - this function may need to upgrade from shared to exclusive locks.

The function implements safe lock upgrading by:
1. **Lock Assessment**: Checks if the current lock level is sufficient (exclusive vs shared)
2. **Safe Upgrade**: If shared lock is held, releases it and reacquires exclusive lock
3. **Race Condition Handling**: After lock upgrade, verifies the split is still incomplete (another process may have completed it)
4. **Delegation**: Calls ginFinishSplit with freestack=false to complete the actual split work

This design prevents deadlocks and race conditions that could occur during concurrent operations while ensuring incomplete splits are properly resolved.

## Parameters / Member Variables
- `btree`: GinBtree structure containing method pointers and index metadata
- `stack`: GinBtreeStack representing the page with incomplete split
- `buildStats`: Statistics structure for tracking index build operations
- `access`: Current lock level (GIN_SHARE or GIN_EXCLUSIVE)

## Dependencies
- Functions called/Symbols referenced:
  - LockBuffer (GIN_UNLOCK, GIN_EXCLUSIVE)
  - GinPageIsIncompleteSplit, BufferGetPage
  - ginFinishSplit
  - elog, RelationGetRelationName
  - INJECTION_POINT (testing framework)
- Called from:
  - ginFindLeafPage (src/backend/access/gin/ginbtree.c:114, 134)
  - ginFindParents (src/backend/access/gin/ginbtree.c:271, 300)
  - ginFinishSplit (src/backend/access/gin/ginbtree.c:706, 730)
  - ginInsertValue (src/backend/access/gin/ginbtree.c:823)

## Notes and Other Information
The function includes debug logging to track incomplete split resolution. The lock upgrade mechanism is designed specifically for insert operations where VACUUM is prevented from running concurrently by holding a cleanup lock on the root. This approach would not be safe during scan operations where concurrent VACUUM could delete pages. The injection point allows testing of incomplete split handling scenarios.