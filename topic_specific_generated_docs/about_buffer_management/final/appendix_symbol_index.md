# Appendix: Symbol Index

[<< Deep Dives](15_deep_dives.md) | [Index](index.md) | [Next: Glossary >>](appendix_glossary.md)

---

Alphabetical index of all documented symbols with source locations and documentation links.

## A

| Symbol | Type | Source | Documented In |
|--------|------|--------|---------------|
| `AddBufferToRing()` | Function | `src/backend/storage/buffer/freelist.c` | [Buffer Replacement Policy](07_buffer_replacement_policy.md) |
| `AtEOXact_LocalBuffers()` | Function | `src/backend/storage/buffer/localbuf.c` | [Local Buffers](13_local_buffers.md) |

## B

| Symbol | Type | Source | Documented In |
|--------|------|--------|---------------|
| `BAS_BULKREAD` | Enum | `src/include/storage/bufmgr.h` | [Buffer Replacement Policy](07_buffer_replacement_policy.md) |
| `BAS_BULKWRITE` | Enum | `src/include/storage/bufmgr.h` | [Buffer Replacement Policy](07_buffer_replacement_policy.md) |
| `BAS_NORMAL` | Enum | `src/include/storage/bufmgr.h` | [Buffer Replacement Policy](07_buffer_replacement_policy.md) |
| `BAS_VACUUM` | Enum | `src/include/storage/bufmgr.h` | [Buffer Replacement Policy](07_buffer_replacement_policy.md) |
| `BM_CHECKPOINT_NEEDED` | Flag | `src/include/storage/buf_internals.h` | [Buffer Pool Architecture](03_buffer_pool_architecture.md) |
| `BM_DIRTY` | Flag | `src/include/storage/buf_internals.h` | [Buffer Pool Architecture](03_buffer_pool_architecture.md) |
| `BM_IO_IN_PROGRESS` | Flag | `src/include/storage/buf_internals.h` | [Page Concurrency Control](06_page_concurrency_control.md) |
| `BM_JUST_DIRTIED` | Flag | `src/include/storage/buf_internals.h` | [Dirty Buffer and Writeback](09_dirty_buffer_and_writeback.md) |
| `BM_LOCKED` | Flag | `src/include/storage/buf_internals.h` | [Page Concurrency Control](06_page_concurrency_control.md) |
| `BM_MAX_USAGE_COUNT` | Constant | `src/include/storage/buf_internals.h` | [Buffer Replacement Policy](07_buffer_replacement_policy.md) |
| `BM_PERMANENT` | Flag | `src/include/storage/buf_internals.h` | [Buffer Pool Architecture](03_buffer_pool_architecture.md) |
| `BM_PIN_COUNT_WAITER` | Flag | `src/include/storage/buf_internals.h` | [Page Concurrency Control](06_page_concurrency_control.md) |
| `BM_TAG_VALID` | Flag | `src/include/storage/buf_internals.h` | [Buffer Pool Architecture](03_buffer_pool_architecture.md) |
| `BM_VALID` | Flag | `src/include/storage/buf_internals.h` | [Buffer Pool Architecture](03_buffer_pool_architecture.md) |
| `BgBufferSync()` | Function | `src/backend/storage/buffer/bufmgr.c` | [Dirty Buffer and Writeback](09_dirty_buffer_and_writeback.md) |
| `BufMappingPartitionLock()` | Inline | `src/include/storage/buf_internals.h` | [Buffer Lookup and Hash Table](04_buffer_lookup_and_hashtable.md) |
| `BufTableDelete()` | Function | `src/backend/storage/buffer/buf_table.c` | [Buffer Lookup and Hash Table](04_buffer_lookup_and_hashtable.md) |
| `BufTableHashCode()` | Function | `src/backend/storage/buffer/buf_table.c` | [Buffer Lookup and Hash Table](04_buffer_lookup_and_hashtable.md) |
| `BufTableInsert()` | Function | `src/backend/storage/buffer/buf_table.c` | [Buffer Lookup and Hash Table](04_buffer_lookup_and_hashtable.md) |
| `BufTableLookup()` | Function | `src/backend/storage/buffer/buf_table.c` | [Buffer Lookup and Hash Table](04_buffer_lookup_and_hashtable.md) |
| `BufferAlloc()` | Function | `src/backend/storage/buffer/bufmgr.c` | [Buffer Access Protocol](05_buffer_access_protocol.md) |
| `BufferDesc` | Struct | `src/include/storage/buf_internals.h` | [Buffer Pool Architecture](03_buffer_pool_architecture.md) |
| `BufferDescPadded` | Union | `src/include/storage/buf_internals.h` | [Buffer Pool Architecture](03_buffer_pool_architecture.md) |
| `BufferGetBlock()` | Inline | `src/include/storage/bufmgr.h` | [Buffer Pool Architecture](03_buffer_pool_architecture.md) |
| `BufferGetLSNAtomic()` | Function | `src/backend/storage/buffer/bufmgr.c` | [WAL Integration](10_wal_integration.md) |
| `BufferLookupEnt` | Struct | `src/backend/storage/buffer/buf_table.c` | [Buffer Lookup and Hash Table](04_buffer_lookup_and_hashtable.md) |
| `BufferStrategyControl` | Struct | `src/backend/storage/buffer/freelist.c` | [Buffer Replacement Policy](07_buffer_replacement_policy.md) |
| `BufferSync()` | Function | `src/backend/storage/buffer/bufmgr.c` | [Dirty Buffer and Writeback](09_dirty_buffer_and_writeback.md) |
| `BufferTag` | Struct | `src/include/storage/buf_internals.h` | [Buffer Pool Architecture](03_buffer_pool_architecture.md) |

## C-F

| Symbol | Type | Source | Documented In |
|--------|------|--------|---------------|
| `ClockSweepTick()` | Inline | `src/backend/storage/buffer/freelist.c` | [Buffer Replacement Policy](07_buffer_replacement_policy.md) |
| `ConditionalLockBuffer()` | Function | `src/backend/storage/buffer/bufmgr.c` | [Page Concurrency Control](06_page_concurrency_control.md) |
| `FlushBuffer()` | Function | `src/backend/storage/buffer/bufmgr.c` | [Dirty Buffer and Writeback](09_dirty_buffer_and_writeback.md) |

## G-I

| Symbol | Type | Source | Documented In |
|--------|------|--------|---------------|
| `GetBufferFromRing()` | Function | `src/backend/storage/buffer/freelist.c` | [Buffer Replacement Policy](07_buffer_replacement_policy.md) |
| `GetVictimBuffer()` | Function | `src/backend/storage/buffer/bufmgr.c` | [Buffer Access Protocol](05_buffer_access_protocol.md) |
| `InitBufTable()` | Function | `src/backend/storage/buffer/buf_table.c` | [Buffer Lookup and Hash Table](04_buffer_lookup_and_hashtable.md) |
| `InitBufferPool()` | Function | `src/backend/storage/buffer/buf_init.c` | [Buffer Pool Architecture](03_buffer_pool_architecture.md) |
| `IssuePendingWritebacks()` | Function | `src/backend/storage/buffer/bufmgr.c` | [Dirty Buffer and Writeback](09_dirty_buffer_and_writeback.md) |

## L

| Symbol | Type | Source | Documented In |
|--------|------|--------|---------------|
| `LocalBufferAlloc()` | Function | `src/backend/storage/buffer/localbuf.c` | [Local Buffers](13_local_buffers.md) |
| `LockBufHdr()` | Function | `src/backend/storage/buffer/bufmgr.c` | [Page Concurrency Control](06_page_concurrency_control.md) |
| `LockBuffer()` | Function | `src/backend/storage/buffer/bufmgr.c` | [Page Concurrency Control](06_page_concurrency_control.md) |
| `LockBufferForCleanup()` | Function | `src/backend/storage/buffer/bufmgr.c` | [Page Concurrency Control](06_page_concurrency_control.md) |

## M-P

| Symbol | Type | Source | Documented In |
|--------|------|--------|---------------|
| `MarkBufferDirty()` | Function | `src/backend/storage/buffer/bufmgr.c` | [Dirty Buffer and Writeback](09_dirty_buffer_and_writeback.md) |
| `MarkBufferDirtyHint()` | Function | `src/backend/storage/buffer/bufmgr.c` | [Dirty Buffer and Writeback](09_dirty_buffer_and_writeback.md) |
| `MdfdVec` | Struct | `src/backend/storage/smgr/md.c` | [Storage Manager](11_storage_manager.md) |
| `PageAddItemExtended()` | Function | `src/backend/storage/page/bufpage.c` | [Page Layout and Types](08_page_layout_and_types.md) |
| `PageGetFreeSpace()` | Function | `src/backend/storage/page/bufpage.c` | [Page Layout and Types](08_page_layout_and_types.md) |
| `PageGetLSN()` | Inline | `src/include/storage/bufpage.h` | [WAL Integration](10_wal_integration.md) |
| `PageHeaderData` | Struct | `src/include/storage/bufpage.h` | [Page Layout and Types](08_page_layout_and_types.md) |
| `PageInit()` | Function | `src/backend/storage/page/bufpage.c` | [Page Layout and Types](08_page_layout_and_types.md) |
| `PageIsVerifiedExtended()` | Function | `src/backend/storage/page/bufpage.c` | [Page Layout and Types](08_page_layout_and_types.md) |
| `PageRepairFragmentation()` | Function | `src/backend/storage/page/bufpage.c` | [Page Layout and Types](08_page_layout_and_types.md) |
| `PageSetChecksumCopy()` | Function | `src/backend/storage/page/bufpage.c` | [Page Layout and Types](08_page_layout_and_types.md) |
| `PageSetLSN()` | Inline | `src/include/storage/bufpage.h` | [WAL Integration](10_wal_integration.md) |
| `PinBuffer()` | Function | `src/backend/storage/buffer/bufmgr.c` | [Page Concurrency Control](06_page_concurrency_control.md) |
| `PinBufferForBlock()` | Function | `src/backend/storage/buffer/bufmgr.c` | [Buffer Access Protocol](05_buffer_access_protocol.md) |
| `PrefetchBuffer()` | Function | `src/backend/storage/buffer/bufmgr.c` | [Access Method Integration](14_access_method_integration.md) |

## R-S

| Symbol | Type | Source | Documented In |
|--------|------|--------|---------------|
| `ReadBuffer()` | Function | `src/backend/storage/buffer/bufmgr.c` | [Buffer Access Protocol](05_buffer_access_protocol.md) |
| `ReadBufferExtended()` | Function | `src/backend/storage/buffer/bufmgr.c` | [Buffer Access Protocol](05_buffer_access_protocol.md) |
| `ReadBuffer_common()` | Function | `src/backend/storage/buffer/bufmgr.c` | [Buffer Access Protocol](05_buffer_access_protocol.md) |
| `ReleaseBuffer()` | Function | `src/backend/storage/buffer/bufmgr.c` | [Page Concurrency Control](06_page_concurrency_control.md) |
| `SMgrRelationData` | Struct | `src/include/storage/smgr.h` | [Storage Manager](11_storage_manager.md) |
| `ScheduleBufferTagForWriteback()` | Function | `src/backend/storage/buffer/bufmgr.c` | [Dirty Buffer and Writeback](09_dirty_buffer_and_writeback.md) |
| `StartBufferIO()` | Function | `src/backend/storage/buffer/bufmgr.c` | [Page Concurrency Control](06_page_concurrency_control.md) |
| `StartReadBuffers()` | Function | `src/backend/storage/buffer/bufmgr.c` | [Buffer Access Protocol](05_buffer_access_protocol.md) |
| `StrategyFreeBuffer()` | Function | `src/backend/storage/buffer/freelist.c` | [Buffer Replacement Policy](07_buffer_replacement_policy.md) |
| `StrategyGetBuffer()` | Function | `src/backend/storage/buffer/freelist.c` | [Buffer Replacement Policy](07_buffer_replacement_policy.md) |
| `StrategyInitialize()` | Function | `src/backend/storage/buffer/freelist.c` | [Buffer Replacement Policy](07_buffer_replacement_policy.md) |
| `StrategyRejectBuffer()` | Function | `src/backend/storage/buffer/freelist.c` | [Buffer Replacement Policy](07_buffer_replacement_policy.md) |
| `StrategySyncStart()` | Function | `src/backend/storage/buffer/freelist.c` | [Buffer Replacement Policy](07_buffer_replacement_policy.md) |
| `SyncOneBuffer()` | Function | `src/backend/storage/buffer/bufmgr.c` | [Dirty Buffer and Writeback](09_dirty_buffer_and_writeback.md) |

## T-Z

| Symbol | Type | Source | Documented In |
|--------|------|--------|---------------|
| `TerminateBufferIO()` | Function | `src/backend/storage/buffer/bufmgr.c` | [Page Concurrency Control](06_page_concurrency_control.md) |
| `UnlockBufHdr()` | Inline | `src/include/storage/buf_internals.h` | [Page Concurrency Control](06_page_concurrency_control.md) |
| `UnpinBuffer()` | Function | `src/backend/storage/buffer/bufmgr.c` | [Page Concurrency Control](06_page_concurrency_control.md) |
| `WaitReadBuffers()` | Function | `src/backend/storage/buffer/bufmgr.c` | [Buffer Access Protocol](05_buffer_access_protocol.md) |
| `WritebackContext` | Struct | `src/include/storage/buf_internals.h` | [Dirty Buffer and Writeback](09_dirty_buffer_and_writeback.md) |
| `XLogFlush()` | Function | `src/backend/access/transam/xlog.c` | [WAL Integration](10_wal_integration.md) |
| `smgropen()` | Function | `src/backend/storage/smgr/smgr.c` | [Storage Manager](11_storage_manager.md) |
| `smgrread()` | Inline | `src/include/storage/smgr.h` | [Storage Manager](11_storage_manager.md) |
| `smgrwrite()` | Inline | `src/include/storage/smgr.h` | [Storage Manager](11_storage_manager.md) |
| `smgrwriteback()` | Function | `src/backend/storage/smgr/smgr.c` | [Storage Manager](11_storage_manager.md) |

---

[<< Deep Dives](15_deep_dives.md) | [Index](index.md) | [Next: Glossary >>](appendix_glossary.md)
