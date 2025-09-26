# ReadBufferExtended

## Location
[src/backend/storage/buffer/bufmgr.c:792-828](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L792-L828)

## Overview
Comprehensive buffer reading function supporting multiple read modes, fork selection, and buffer access strategies for relation block access.

## Definition
inline Buffer ReadBufferExtended(Relation reln, ForkNumber forkNum, BlockNumber blockNum, ReadBufferMode mode, BufferAccessStrategy strategy)

## Detailed Description
ReadBufferExtended is the primary interface for reading blocks from relation files with full control over reading behavior. It supports multiple read modes including normal validation (RBM_NORMAL), zero-on-error recovery (RBM_ZERO_ON_ERROR), and zero-and-lock modes (RBM_ZERO_AND_LOCK, RBM_ZERO_AND_CLEANUP_LOCK) for different use cases. The function can handle special block number P_NEW for relation extension and supports custom buffer access strategies for controlling replacement policies.

The function includes security validation to prevent access to temporary tables from other sessions, maintaining data isolation. It serves as the foundation for all buffer reading operations in PostgreSQL, with other buffer functions typically calling this implementation with specific parameter combinations.

Different read modes serve distinct purposes: normal mode for standard data access with validation, zero-on-error for non-critical data where corruption recovery is acceptable, and zero-and-lock modes for cases where the caller will completely rewrite the page content.

## Parameters / Member Variables
- reln: Relation structure containing metadata about the target relation
- forkNum: Fork number identifying which fork to read (main, FSM, VM, etc.)
- blockNum: Block number within the fork, or P_NEW for relation extension
- mode: Read mode controlling validation and behavior (RBM_NORMAL, RBM_ZERO_ON_ERROR, etc.)
- strategy: Buffer access strategy for controlling replacement policy, or NULL for default

## Dependencies
- Functions called/Symbols referenced:
  - RELATION_IS_OTHER_TEMP: Security check for temporary table access
  - [ReadBuffer_common](ReadBuffer_common.md): Core buffer reading implementation
  - [RelationGetSmgr](RelationGetSmgr.md): Gets storage manager handle for relation
- Called from (representative examples):
  - [ReadBuffer](ReadBuffer.md): Simplified buffer reading interface
  - [brin_vacuum_scan](../b/brin_vacuum_scan.md): BRIN index vacuum operations
  - [ginDeletePage](../g/ginDeletePage.md): GIN index page deletion
  - [hashbulkdelete](../h/hashbulkdelete.md): Hash index bulk deletion
  - [lazy_scan_heap](../l/lazy_scan_heap.md): Heap vacuum scanning
  - [vm_readbuf](../v/vm_readbuf.md): Visibility map buffer reading
  - [fsm_readbuf](../f/fsm_readbuf.md): Free space map buffer reading

## Notes and Other Information
- Marked as inline for performance in frequently called code paths
- Returns pinned buffer that must be released by caller using ReleaseBuffer
- P_NEW block number triggers relation extension rather than reading existing block
- Zero-and-lock modes return locked buffers to prevent concurrent access during initialization
- Buffer access strategies allow fine-tuning of replacement policies for bulk operations
- All-zero pages are considered valid in normal mode through PageIsVerifiedExtended
- Direct entry point for most specialized buffer access requirements in PostgreSQL