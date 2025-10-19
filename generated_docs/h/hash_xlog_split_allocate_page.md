# hash_xlog_split_allocate_page

## Location
[src/backend/access/hash/hash_xlog.c:311-427](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hash_xlog.c#L311-L427)

## Overview
Replays the page allocation phase of a hash index bucket split operation during WAL recovery, setting up the old and new bucket pages and updating metapage structures.

## Definition

```c
static void
hash_xlog_split_allocate_page(XLogReaderState *record)
```
## Detailed Description
This function handles WAL replay for the allocation phase of hash index bucket splitting. Hash index bucket splitting is a complex operation that occurs when buckets become full and need to be divided. This function specifically handles the page allocation and initial setup phase, preparing both the old bucket page (which will have some tuples redistributed) and the new bucket page (which will receive redistributed tuples).

The function operates on three buffers: it updates the old bucket page's special space to set appropriate flags and establish linkage to the new bucket; it initializes a new bucket page with proper bucket number and flags; and it updates the metapage to reflect the new maximum bucket number and potentially update hash masks and overflow point information. The function uses cleanup locks on both bucket pages to maintain consistency with normal operation patterns.

## Parameters / Member Variables
- `*record`: XLogReaderState pointer containing the WAL record with split allocation data including old_bucket_flag, new_bucket, new_bucket_flag, and optional mask/splitpoint update flags
## Dependencies
- Functions called/Symbols referenced:
  - [xl_hash_split_allocate_page](../x/xl_hash_split_allocate_page.md) (WAL record structure)
  - XLogRecGetData (extracts record data)
  - [XLogReadBufferForRedoExtended](../X/XLogReadBufferForRedoExtended.md) (reads buffer with extended options)
  - [XLogReadBufferForRedo](../X/XLogReadBufferForRedo.md) (reads buffer for redo)
  - [XLogRecGetBlockData](../X/XLogRecGetBlockData.md) (gets block data from record)
  - [_hash_initbuf](_hash_initbuf.md) (initializes hash page buffer)
  - HashPageGetOpaque (gets page opaque data)
  - HashPageGetMeta (gets metapage metadata)
  - RBM_NORMAL (normal buffer mode)
  - RBM_ZERO_AND_CLEANUP_LOCK (zero and cleanup lock mode)
  - BLK_NEEDS_REDO (indicates block needs redo)
  - BLK_RESTORED (indicates block was restored)
  - XLH_SPLIT_META_UPDATE_MASKS (flag for mask updates)
  - XLH_SPLIT_META_UPDATE_SPLITPOINT (flag for splitpoint updates)
- Called from:
  - [hash_redo](hash_redo.md) (main hash WAL replay function)

## Notes and Other Information
- This is a static function used only within the hash WAL recovery subsystem
- The function is part of a multi-phase bucket splitting operation and handles only the allocation phase
- Cleanup locks are taken on both old and new buckets to maintain consistency with normal operation
- The old bucket page's special space is updated even when restored from a full page image since special space isn't included
- Metapage updates are conditional based on flags in the WAL record, allowing for selective updates of hash masks and overflow points
- The function handles complex metapage field updates including hashm_maxbucket, hashm_lowmask, hashm_highmask, hashm_spares, and hashm_ovflpoint
- Buffer release follows a specific order to maintain consistency with normal operation patterns

## Simplified Source

```c
static void
hash_xlog_split_allocate_page(XLogReaderState *record)
{
    XLogRecPtr lsn = record->EndRecPtr;
    xl_hash_split_allocate_page *xlrec = (xl_hash_split_allocate_page *) XLogRecGetData(record);
    Buffer oldbuf;
    Buffer newbuf;
    Buffer metabuf;
    char *data;
    XLogRedoAction action;

    // Update old bucket page with split information
    action = XLogReadBufferForRedoExtended(record, 0, RBM_NORMAL, true, &oldbuf);
    if (action == BLK_NEEDS_REDO || action == BLK_RESTORED)
    {
        Page oldpage = BufferGetPage(oldbuf);
        HashPageOpaque oldopaque = HashPageGetOpaque(oldpage);

        // Set flags and link to new bucket
        oldopaque->hasho_flag = xlrec->old_bucket_flag;
        oldopaque->hasho_prevblkno = xlrec->new_bucket;

        PageSetLSN(oldpage, lsn);
        MarkBufferDirty(oldbuf);
    }

    // Initialize new bucket page
    XLogReadBufferForRedoExtended(record, 1, RBM_ZERO_AND_CLEANUP_LOCK, true, &newbuf);
    _hash_initbuf(newbuf, xlrec->new_bucket, xlrec->new_bucket,
                  xlrec->new_bucket_flag, true);
    MarkBufferDirty(newbuf);
    PageSetLSN(BufferGetPage(newbuf), lsn);

    // Release bucket buffers
    if (BufferIsValid(oldbuf))
        UnlockReleaseBuffer(oldbuf);
    if (BufferIsValid(newbuf))
        UnlockReleaseBuffer(newbuf);

    // Update metapage with new bucket information
    if (XLogReadBufferForRedo(record, 2, &metabuf) == BLK_NEEDS_REDO)
    {
        Page page = BufferGetPage(metabuf);
        HashMetaPage metap = HashPageGetMeta(page);
        Size datalen;

        // Update maximum bucket number
        metap->hashm_maxbucket = xlrec->new_bucket;

        data = XLogRecGetBlockData(record, 2, &datalen);

        // Update hash masks if needed
        if (xlrec->flags & XLH_SPLIT_META_UPDATE_MASKS)
        {
            uint32 lowmask;
            uint32 *highmask;

            memcpy(&lowmask, data, sizeof(uint32));
            highmask = (uint32 *) ((char *) data + sizeof(uint32));

            metap->hashm_lowmask = lowmask;
            metap->hashm_highmask = *highmask;
            data += sizeof(uint32) * 2;
        }

        // Update overflow point if needed
        if (xlrec->flags & XLH_SPLIT_META_UPDATE_SPLITPOINT)
        {
            uint32 ovflpoint;
            uint32 *ovflpages;

            memcpy(&ovflpoint, data, sizeof(uint32));
            ovflpages = (uint32 *) ((char *) data + sizeof(uint32));

            metap->hashm_spares[ovflpoint] = *ovflpages;
            metap->hashm_ovflpoint = ovflpoint;
        }

        MarkBufferDirty(metabuf);
        PageSetLSN(BufferGetPage(metabuf), lsn);
    }

    if (BufferIsValid(metabuf))
        UnlockReleaseBuffer(metabuf);
}
```