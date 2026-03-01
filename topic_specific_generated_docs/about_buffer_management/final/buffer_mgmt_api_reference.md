# Buffer Management API Reference

[<< Quick Reference](buffer_mgmt_quick_reference.md) | [Index](index.md) | [Next: Quality Report >>](quality_report.md)

---

Function signatures grouped by subsystem. All source paths are relative to the PostgreSQL source root.

## Buffer Access (src/backend/storage/buffer/bufmgr.c)

### Public Read API

```c
Buffer ReadBuffer(Relation reln, BlockNumber blockNum);

Buffer ReadBufferExtended(Relation reln, ForkNumber forkNum,
                          BlockNumber blockNum, ReadBufferMode mode,
                          BufferAccessStrategy strategy);

Buffer ReadBufferWithoutRelcache(RelFileLocator rlocator,
                                 ForkNumber forkNum, BlockNumber blockNum,
                                 ReadBufferMode mode, BufferAccessStrategy strategy,
                                 bool permanent);
```

### Vectorized Read API

```c
bool StartReadBuffer(ReadBuffersOperation *operation,
                     Buffer *buffer, BlockNumber blockNum, int flags);

bool StartReadBuffers(ReadBuffersOperation *operation,
                      Buffer *buffers, BlockNumber blockNum,
                      int *nblocks, int flags);

void WaitReadBuffers(ReadBuffersOperation *operation);
```

### Internal Allocation

```c
static pg_attribute_always_inline BufferDesc *
BufferAlloc(SMgrRelation smgr, char relpersistence, ForkNumber forkNum,
            BlockNumber blockNum, BufferAccessStrategy strategy,
            bool *foundPtr, IOContext io_context);

static Buffer GetVictimBuffer(BufferAccessStrategy strategy,
                               IOContext io_context);

static pg_attribute_always_inline Buffer
PinBufferForBlock(Relation rel, SMgrRelation smgr, char smgr_persistence,
                  ForkNumber forkNum, BlockNumber blockNum,
                  BufferAccessStrategy strategy, bool *foundPtr);
```

## Pin Management (src/backend/storage/buffer/bufmgr.c)

```c
static bool PinBuffer(BufferDesc *buf, BufferAccessStrategy strategy);

static void PinBuffer_Locked(BufferDesc *buf);

static void UnpinBufferNoOwner(BufferDesc *buf);

void ReleaseBuffer(Buffer buffer);

void UnlockReleaseBuffer(Buffer buffer);

void IncrBufferRefCount(Buffer buffer);
```

## Content Lock Management (src/backend/storage/buffer/bufmgr.c)

```c
void LockBuffer(Buffer buffer, int mode);
    /* mode: BUFFER_LOCK_UNLOCK (0), BUFFER_LOCK_SHARE (1), BUFFER_LOCK_EXCLUSIVE (2) */

bool ConditionalLockBuffer(Buffer buffer);

void LockBufferForCleanup(Buffer buffer);

bool HoldingBufferPinThatDelaysRecovery(void);
```

## Dirty Buffer Management (src/backend/storage/buffer/bufmgr.c)

```c
void MarkBufferDirty(Buffer buffer);

void MarkBufferDirtyHint(Buffer buffer, bool buffer_std);

static void FlushBuffer(BufferDesc *buf, SMgrRelation reln,
                        IOObject io_object, IOContext io_context);

void FlushRelationBuffers(Relation rel);

void FlushRelationsAllBuffers(struct SMgrRelationData **smgrs, int nrels);
```

## I/O Coordination (src/backend/storage/buffer/bufmgr.c)

```c
static bool StartBufferIO(BufferDesc *buf, bool forInput, bool nowait);

static void TerminateBufferIO(BufferDesc *buf, bool clear_dirty,
                              uint32 set_flag_bits, bool forget_owner);

static void WaitIO(BufferDesc *buf);
```

## Header Spinlock (src/backend/storage/buffer/bufmgr.c)

```c
uint32 LockBufHdr(BufferDesc *desc);
```

```c
/* Inline in src/include/storage/buf_internals.h */
static inline void UnlockBufHdr(BufferDesc *desc, uint32 buf_state);
```

## Background Writer and Checkpoint (src/backend/storage/buffer/bufmgr.c)

```c
bool BgBufferSync(WritebackContext *wb_context);

static int SyncOneBuffer(int buf_id, bool skip_recently_used,
                         WritebackContext *wb_context);

void CheckPointBuffers(int flags);

static void BufferSync(int flags);
```

## Writeback Advisory (src/backend/storage/buffer/bufmgr.c)

```c
void ScheduleBufferTagForWriteback(WritebackContext *wb_context,
                                   IOContext io_context, BufferTag *tag);

void IssuePendingWritebacks(WritebackContext *wb_context,
                            IOContext io_context);
```

## LSN Operations (src/include/storage/bufpage.h)

```c
static inline XLogRecPtr PageGetLSN(Page page);

static inline void PageSetLSN(Page page, XLogRecPtr lsn);

XLogRecPtr BufferGetLSNAtomic(Buffer buffer);
```

## Prefetch (src/backend/storage/buffer/bufmgr.c)

```c
PrefetchBufferResult PrefetchBuffer(Relation reln, ForkNumber forkNum,
                                    BlockNumber blockNum);
```

## Hash Table (src/backend/storage/buffer/buf_table.c)

```c
void InitBufTable(int size);

uint32 BufTableHashCode(BufferTag *tagPtr);

int BufTableLookup(BufferTag *tagPtr, uint32 hashcode);

int BufTableInsert(BufferTag *tagPtr, uint32 hashcode, int buf_id);

void BufTableDelete(BufferTag *tagPtr, uint32 hashcode);
```

## Replacement Strategy (src/backend/storage/buffer/freelist.c)

```c
BufferDesc *StrategyGetBuffer(BufferAccessStrategy strategy,
                              uint32 *buf_state, bool *from_ring);

void StrategyFreeBuffer(BufferDesc *buf);

int StrategySyncStart(uint32 *complete_passes, uint32 *num_buf_alloc);

void StrategyNotifyBgWriter(int bgwprocno);

BufferAccessStrategy GetAccessStrategy(BufferAccessStrategyType btype);

BufferAccessStrategy GetAccessStrategyWithSize(BufferAccessStrategyType btype,
                                                int ring_size_kb);

void FreeAccessStrategy(BufferAccessStrategy strategy);

bool StrategyRejectBuffer(BufferAccessStrategy strategy,
                          BufferDesc *buf, bool from_ring);
```

## Page Operations (src/backend/storage/page/bufpage.c)

```c
void PageInit(Page page, Size pageSize, Size specialSize);

OffsetNumber PageAddItemExtended(Page page, Item item, Size size,
                                 OffsetNumber offsetNumber, int flags);

void PageRepairFragmentation(Page page);

Size PageGetFreeSpace(Page page);

Size PageGetHeapFreeSpace(Page page);

bool PageIsVerifiedExtended(Page page, BlockNumber blkno, int flags);

char *PageSetChecksumCopy(Page page, BlockNumber blkno);

void PageSetChecksumInplace(Page page, BlockNumber blkno);
```

## Initialization (src/backend/storage/buffer/buf_init.c)

```c
void InitBufferPool(void);

Size BufferShmemSize(void);
```

## Storage Manager (src/backend/storage/smgr/smgr.c)

```c
SMgrRelation smgropen(RelFileLocator rlocator, ProcNumber backend);

void smgrclose(SMgrRelation reln);

static inline void smgrread(SMgrRelation reln, ForkNumber forknum,
                            BlockNumber blocknum, void *buffer);

void smgrreadv(SMgrRelation reln, ForkNumber forknum,
               BlockNumber blocknum, void **buffers, BlockNumber nblocks);

static inline void smgrwrite(SMgrRelation reln, ForkNumber forknum,
                             BlockNumber blocknum, const void *buffer,
                             bool skipFsync);

void smgrwritev(SMgrRelation reln, ForkNumber forknum,
                BlockNumber blocknum, const void **buffers,
                BlockNumber nblocks, bool skipFsync);

void smgrextend(SMgrRelation reln, ForkNumber forknum,
                BlockNumber blocknum, const void *buffer, bool skipFsync);

BlockNumber smgrnblocks(SMgrRelation reln, ForkNumber forknum);

void smgrwriteback(SMgrRelation reln, ForkNumber forknum,
                   BlockNumber blocknum, BlockNumber nblocks);

void smgrimmedsync(SMgrRelation reln, ForkNumber forknum);

bool smgrprefetch(SMgrRelation reln, ForkNumber forknum,
                  BlockNumber blocknum, int nblocks);
```

## Local Buffers (src/backend/storage/buffer/localbuf.c)

```c
BufferDesc *LocalBufferAlloc(SMgrRelation smgr, ForkNumber forkNum,
                             BlockNumber blockNum, bool *foundPtr);

bool PinLocalBuffer(BufferDesc *buf_hdr, bool adjust_usagecount);

void UnpinLocalBuffer(Buffer buffer);

void MarkLocalBufferDirty(Buffer buffer);

void AtEOXact_LocalBuffers(bool isCommit);

void AtProcExit_LocalBuffers(void);
```

---

[<< Quick Reference](buffer_mgmt_quick_reference.md) | [Index](index.md) | [Next: Quality Report >>](quality_report.md)
