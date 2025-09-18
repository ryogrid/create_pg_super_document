# dlist_init

## Location
[src/include/lib/ilist.h:314-324](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/ilist.h#L314-L324)

## Overview
Initializes a doubly-linked list head structure to establish an empty list state with proper circular references.

## Definition


## Detailed Description
The  function initializes a doubly-linked list by setting up the head node to point to itself in both forward and backward directions. This creates a circular reference pattern where an empty list has its head node's  and  pointers both pointing to the head node itself. This design simplifies list operations by eliminating special cases for empty lists, as the head always has valid next/previous pointers. The function discards any previous state without cleanup, making it suitable for fresh initialization but requiring careful use when reinitializing existing lists.

## Parameters / Member Variables
- : Pointer to the  structure that will be initialized as an empty doubly-linked list

## Dependencies
- Functions called/Symbols referenced:
  - [dlist_head](dlist_head.md) (structure type)
- Called from (representative examples):
  - [disassembleLeaf](disassembleLeaf.md) (src/backend/access/gin/gindatapage.c:1378)
  - [MarkAsPreparingGuts](../M/MarkAsPreparingGuts.md) (src/backend/access/transam/twophase.c:476)
  - [XLogPrefetcherAllocate](../X/XLogPrefetcherAllocate.md) (src/backend/access/transam/xlogprefetcher.c:376)
  - [cache_purge_all](../c/cache_purge_all.md) (src/backend/executor/nodeMemoize.c:420)
  - [ExecInitMemoize](../E/ExecInitMemoize.md) (src/backend/executor/nodeMemoize.c:1046)
  - [AutoVacuumShmemInit](../A/AutoVacuumShmemInit.md) (src/backend/postmaster/autovacuum.c:3336)
  - [ReorderBufferAllocate](../R/ReorderBufferAllocate.md) (src/backend/replication/logical/reorderbuffer.c:395)
  - InitProcGlobal (src/backend/storage/lmgr/proc.c:174)

## Notes and Other Information
- The function is implemented as a static inline function for performance efficiency
- Previous state is discarded without cleanup - ensure proper cleanup before reinitializing used lists
- The circular reference design (head pointing to itself when empty) is a key feature that simplifies many list operations
- Commonly used in PostgreSQL's memory management, transaction processing, and storage subsystems
- Located in src/include/lib/ilist.h:314-324