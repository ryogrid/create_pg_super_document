# Buffer Lookup and Hash Table

[<< Buffer Pool Architecture](03_buffer_pool_architecture.md) | [Index](index.md) | [Next: Buffer Access Protocol >>](05_buffer_access_protocol.md)

---

## Overview

The buffer lookup hash table is a partitioned shared-memory hash table that maps [BufferTag](03_buffer_pool_architecture.md) values (page identifiers) to buffer IDs (`buf_id`). It is the mechanism by which the buffer manager determines whether a requested page is already present in the buffer pool. The hash table is partitioned into `NUM_BUFFER_PARTITIONS` (128) independent segments, each protected by its own LWLock, to reduce contention in concurrent workloads.

All hash table operations are implemented in `src/backend/storage/buffer/buf_table.c` (162 lines).

## Data Structures

### BufferLookupEnt

The hash table entry type (source: `src/backend/storage/buffer/buf_table.c`):

```c
typedef struct
{
    BufferTag   key;    /* Tag of a disk page */
    int         id;     /* Associated buffer ID */
} BufferLookupEnt;
```

### SharedBufHash

The hash table itself is a standard PostgreSQL `HTAB` allocated in shared memory:

```c
static HTAB *SharedBufHash;
```

## Partition-Based Locking

The hash table uses `NUM_BUFFER_PARTITIONS` (128, a power of 2) separate LWLock partitions to allow concurrent access. The partition for a given tag is determined by its hash code:

```c
/* From src/include/storage/buf_internals.h */
static inline uint32
BufTableHashPartition(uint32 hashcode)
{
    return hashcode % NUM_BUFFER_PARTITIONS;
}

static inline LWLock *
BufMappingPartitionLock(uint32 hashcode)
{
    return &MainLWLockArray[BUFFER_MAPPING_LWLOCK_OFFSET +
                            BufTableHashPartition(hashcode)].lock;
}
```

Lock modes:
- **Shared (LW_SHARED)**: Sufficient for lookup operations (`BufTableLookup`).
- **Exclusive (LW_EXCLUSIVE)**: Required for insert and delete operations (`BufTableInsert`, `BufTableDelete`).

If multiple partition locks must be held simultaneously, they must be acquired in partition-number order to prevent deadlock. See [Page Concurrency Control](06_page_concurrency_control.md) for lock ordering rules.

## Core API

### InitBufTable()

Source: `src/backend/storage/buffer/buf_table.c:50`

```c
void InitBufTable(int size)
```

Initializes the shared hash table. Called by [StrategyInitialize()](07_buffer_replacement_policy.md) during postmaster startup.

The hash table is created with `HASH_PARTITION` flag to enable partitioned locking:

```c
info.num_partitions = NUM_BUFFER_PARTITIONS;
SharedBufHash = ShmemInitHash("Shared Buffer Lookup Table",
                              size, size, &info,
                              HASH_ELEM | HASH_BLOBS | HASH_PARTITION);
```

The table is sized to `NBuffers + NUM_BUFFER_PARTITIONS` because during [BufferAlloc()](05_buffer_access_protocol.md), a new entry is inserted before the old entry (from the victim buffer) is deleted. This can occur concurrently in each partition, requiring the extra headroom.

### BufTableHashCode()

Source: `src/backend/storage/buffer/buf_table.c:77`

```c
uint32 BufTableHashCode(BufferTag *tagPtr)
```

Computes the hash code for a `BufferTag`. The hash code is computed once and then reused for partition lock selection, lookup, insert, and delete operations. This avoids redundant hash computation (noted in the source: "we don't want to do the hash computation twice (hash_any is a bit slow)").

**Returns:** 32-bit hash code.

### BufTableLookup()

Source: `src/backend/storage/buffer/buf_table.c:89`

```c
int BufTableLookup(BufferTag *tagPtr, uint32 hashcode)
```

Looks up a `BufferTag` in the hash table. The caller must hold at least a shared lock on the appropriate partition lock.

**Returns:** Buffer ID (>= 0) if found, -1 if not found.

### BufTableInsert()

Source: `src/backend/storage/buffer/buf_table.c:117`

```c
int BufTableInsert(BufferTag *tagPtr, uint32 hashcode, int buf_id)
```

Inserts a new tag-to-buffer mapping. If a conflicting entry already exists (another backend inserted the same tag concurrently), returns the existing buffer ID instead of inserting. The caller must hold an exclusive lock on the appropriate partition lock.

**Returns:** -1 on successful insertion. If a conflict exists, returns the conflicting buffer ID.

### BufTableDelete()

Source: `src/backend/storage/buffer/buf_table.c:147`

```c
void BufTableDelete(BufferTag *tagPtr, uint32 hashcode)
```

Deletes a hash table entry. The caller must hold an exclusive lock on the appropriate partition lock. Raises an ERROR if the entry does not exist (indicating hash table corruption).

## Interaction with BufferAlloc

The hash table is the critical data structure in [BufferAlloc()](05_buffer_access_protocol.md) (source: `src/backend/storage/buffer/bufmgr.c:1594`). The protocol is:

### Step 1: Compute Tag and Hash

```c
InitBufferTag(&newTag, &smgr->smgr_rlocator.locator, forkNum, blockNum);
newHash = BufTableHashCode(&newTag);
newPartitionLock = BufMappingPartitionLock(newHash);
```

### Step 2: Lookup Under Shared Lock

```c
LWLockAcquire(newPartitionLock, LW_SHARED);
existing_buf_id = BufTableLookup(&newTag, newHash);
```

If found, pin the buffer and release the partition lock. This is the common fast path.

### Step 3: Miss -- Acquire Victim and Insert Under Exclusive Lock

If not found, release the shared lock, obtain a victim buffer via [GetVictimBuffer()](05_buffer_access_protocol.md), then re-acquire the partition lock exclusively and attempt insertion:

```c
LWLockAcquire(newPartitionLock, LW_EXCLUSIVE);
existing_buf_id = BufTableInsert(&newTag, newHash, victim_buf_hdr->buf_id);
```

### Step 4: Handle Race Condition

If `BufTableInsert()` returns a non-negative value, another backend inserted the same tag between our lookup and insert. In this case:

1. Unpin and free the victim buffer (return it to the freelist via [StrategyFreeBuffer()](07_buffer_replacement_policy.md)).
2. Pin the already-inserted buffer.
3. Release the partition lock.

This "lookup-before-insert" protocol with optimistic concurrency avoids holding exclusive partition locks during the common-case buffer hit.

## Performance Characteristics

- **Lookup (cache hit)**: One shared partition lock acquisition + hash probe. This is the common case and is highly concurrent -- 128 independent partitions mean minimal contention.

- **Insert (cache miss)**: One exclusive partition lock acquisition + hash insertion. Exclusive locks serialize inserts to the same partition, but misses to different partitions proceed in parallel.

- **Hash function**: Uses PostgreSQL's general-purpose `hash_any()` function on the raw bytes of the `BufferTag` structure (20 bytes). This is noted as "a bit slow" in the source comments, which is why the hash code is computed once and passed to all subsequent operations.

- **Memory overhead**: Each `BufferLookupEnt` is approximately 24 bytes (20-byte tag + 4-byte ID). For 16,384 buffers, the hash table itself consumes roughly 600 KB including overhead.

## Locking Rules

From `src/backend/storage/buffer/README`:

1. A buffer found in the hash table must be pinned before releasing the partition lock. Otherwise another backend could evict it.

2. To alter the page assignment of any buffer (change its tag), one must hold the exclusive partition lock spanning both the adjustment of header fields and the hash table modification.

3. If multiple partition locks are needed, they must be acquired in partition-number order.

4. The common code path (buffer hit) needs only a shared partition lock, avoiding serialization.

See also: [Page Concurrency Control](06_page_concurrency_control.md) for the complete lock hierarchy.

---

[<< Buffer Pool Architecture](03_buffer_pool_architecture.md) | [Index](index.md) | [Next: Buffer Access Protocol >>](05_buffer_access_protocol.md)
