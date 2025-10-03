# _hash_vacuum_one_page

## Location
[src/backend/access/hash/hashinsert.c:370-465](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashinsert.c#L370-L465)

## Overview
The  function removes dead tuples (LP_DEAD items) from a single hash index page to reclaim space, used opportunistically during insertions when cleanup locks are available.

## Definition

```c
static void
_hash_vacuum_one_page(Relation rel, Relation hrel, Buffer metabuf, Buffer buf)
```
## Detailed Description
This function performs localized vacuum operations on a single hash index page by removing tuples marked as LP_DEAD. It is called opportunistically during insertion operations when:

1. A page lacks sufficient space for a new tuple
2. The page contains dead tuples (indicated by LH_PAGE_HAS_DEAD_TUPLES flag)
3. A cleanup lock is available on the page

The vacuum process involves:

1. **Dead tuple identification**: Scanning all tuples on the page to identify those marked with LP_DEAD status
2. **Conflict horizon calculation**: Computing the snapshot conflict horizon for logical decoding to ensure proper handling on standby servers
3. **Bulk deletion**: Removing all identified dead tuples in a single operation
4. **Metadata updates**: 
   - Clearing the LH_PAGE_HAS_DEAD_TUPLES flag on the page
   - Decrementing the global tuple count in the metapage
5. **WAL logging**: Recording the vacuum operation for crash recovery and replication

The function is designed to be lightweight and non-blocking, only performing cleanup when conditions are favorable.

## Parameters / Member Variables
- `rel`: The hash index relation being vacuumed
- `hrel`: The heap relation (used for snapshot conflict horizon calculation)
- `metabuf`: Buffer containing the hash index metapage (for tuple count updates)
- `buf`: Buffer containing the page to be vacuumed (must have cleanup lock)
## Dependencies
- Functions called/Symbols referenced:
  - , : Page inspection functions
  - : Check if tuple is marked as dead
  - : Calculate snapshot conflict horizon
  - : Bulk delete dead tuples
  - , : Access hash-specific page structures
  - : Check if WAL logging is required
  - , , : WAL logging functions
  - Various buffer management functions for locking and marking dirty

- Called from (representative examples):
  - : During tuple insertion when space is needed

## Notes and Other Information
- This is a static function, only callable within the same source file
- Requires a cleanup lock on the target page before being called
- The function is atomic - either all dead tuples are removed or none
- Updates the global tuple count in the metapage, requiring exclusive lock on metabuf
- The LH_PAGE_HAS_DEAD_TUPLES flag clearing is optimistic - there might be newly dead tuples not included in this cleanup
- WAL logging includes both the vacuum operation details and the array of deleted offset numbers for standby server processing
- Uses critical sections to ensure atomicity of the cleanup operation
- The snapshot conflict horizon is important for logical replication to handle conflicts properly on standby servers
- This function provides an efficient way to reclaim space without requiring a full vacuum operation

## Simplified Source

```c
// Vacuum just one index page - remove LP_DEAD items to reclaim space.
// Must acquire cleanup lock on the page before calling this function.
static void _hash_vacuum_one_page(Relation rel, Relation hrel,
                                  Buffer metabuf, Buffer buf) {
    OffsetNumber deletable[MaxOffsetNumber];
    int ndeletable = 0;
    Page page = BufferGetPage(buf);
    HashPageOpaque pageopaque;
    HashMetaPage metap;

    // Scan page to find all dead tuples
    OffsetNumber maxoff = PageGetMaxOffsetNumber(page);
    for (OffsetNumber offnum = FirstOffsetNumber;
         offnum <= maxoff;
         offnum = OffsetNumberNext(offnum)) {

        ItemId itemId = PageGetItemId(page, offnum);
        if (ItemIdIsDead(itemId))
            deletable[ndeletable++] = offnum;
    }

    if (ndeletable > 0) {
        // Calculate snapshot conflict horizon for logical decoding
        TransactionId snapshotConflictHorizon =
            index_compute_xid_horizon_for_tuples(rel, hrel, buf,
                                                deletable, ndeletable);

        // Lock metapage to update tuple count
        LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);
        START_CRIT_SECTION();

        // Remove all dead tuples from page
        PageIndexMultiDelete(page, deletable, ndeletable);

        // Clear the "has dead tuples" flag (optimistic - might be newly dead ones)
        pageopaque = HashPageGetOpaque(page);
        pageopaque->hasho_flag &= ~LH_PAGE_HAS_DEAD_TUPLES;

        // Decrement global tuple count
        metap = HashPageGetMeta(BufferGetPage(metabuf));
        metap->hashm_ntuples -= ndeletable;

        MarkBufferDirty(buf);
        MarkBufferDirty(metabuf);

        // WAL logging for crash recovery and replication
        if (RelationNeedsWAL(rel)) {
            xl_hash_vacuum_one_page xlrec;
            xlrec.isCatalogRel = RelationIsAccessibleInLogicalDecoding(hrel);
            xlrec.snapshotConflictHorizon = snapshotConflictHorizon;
            xlrec.ntuples = ndeletable;

            XLogBeginInsert();
            XLogRegisterBuffer(0, buf, REGBUF_STANDARD);
            XLogRegisterData((char *) &xlrec, SizeOfHashVacuumOnePage);
            XLogRegisterData((char *) deletable,
                           ndeletable * sizeof(OffsetNumber));
            XLogRegisterBuffer(1, metabuf, REGBUF_STANDARD);

            XLogRecPtr recptr = XLogInsert(RM_HASH_ID, XLOG_HASH_VACUUM_ONE_PAGE);
            PageSetLSN(BufferGetPage(buf), recptr);
            PageSetLSN(BufferGetPage(metabuf), recptr);
        }

        END_CRIT_SECTION();
        LockBuffer(metabuf, BUFFER_LOCK_UNLOCK);
    }
}
```