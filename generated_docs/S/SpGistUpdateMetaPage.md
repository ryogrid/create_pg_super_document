# SpGistUpdateMetaPage

## Location
[src/backend/access/spgist/spgutils.c:442-481](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgutils.c#L442-L481)

## Overview
Updates the SP-GiST index metapage with the latest lastUsedPages information from the local cache, using non-blocking operations to maintain high concurrency.

## Definition
void SpGistUpdateMetaPage(Relation index)

## Detailed Description
This function performs a best-effort update of the SP-GiST index metapage with cached lastUsedPages information. It is designed to be non-critical and non-blocking to avoid impacting index performance. The function uses ConditionalLockBuffer to avoid waiting for locks, and deliberately does not WAL-log changes to reduce write-ahead log traffic.

The function also includes a compatibility fix for pre-PostgreSQL 11 versions by correctly setting the page's pd_lower field. This ensures that metadata is preserved during page compression operations by xlog.c, addressing issues with pg_upgraded indexes that might contain incorrect pd_lower values.

## Parameters / Member Variables
- : Relation object representing the SP-GiST index whose metapage should be updated

## Dependencies
- Functions called/Symbols referenced:
  - [ReadBuffer](../R/ReadBuffer.md)
  - [ConditionalLockBuffer](../C/ConditionalLockBuffer.md)
  - [BufferGetPage](../B/BufferGetPage.md)
  - SpGistPageGetMeta
  - [MarkBufferDirty](../M/MarkBufferDirty.md)
  - [UnlockReleaseBuffer](../U/UnlockReleaseBuffer.md)
  - [ReleaseBuffer](../R/ReleaseBuffer.md)
- Called from (representative examples):
  - [spgbuild](../s/spgbuild.md)
  - [spginsert](../s/spginsert.md)
  - [spgvacuumscan](../s/spgvacuumscan.md)

## Notes and Other Information
This is an optimization function that operates on a "best effort" basis - if the metapage cannot be locked immediately, the update is simply skipped without error. The lastUsedPages information helps with efficient page allocation but is not critical for index correctness. The function includes important backward compatibility handling for pg_upgraded indexes from pre-v11 PostgreSQL versions.

## Simplified Source

```c
void SpGistUpdateMetaPage(Relation index) {
    SpGistCache *cache = (SpGistCache *) index->rd_amcache;

    if (cache != NULL) {
        Buffer metabuffer = ReadBuffer(index, SPGIST_METAPAGE_BLKNO);

        // Try to lock the metapage buffer (non-blocking)
        if (ConditionalLockBuffer(metabuffer)) {
            Page metapage = BufferGetPage(metabuffer);
            SpGistMetaPageData *metadata = SpGistPageGetMeta(metapage);

            // Update cached page info to metapage
            metadata->lastUsedPages = cache->lastUsedPages;

            // Fix pd_lower for compatibility with pre-v11 pg_upgraded indexes
            ((PageHeader) metapage)->pd_lower =
                ((char *) metadata + sizeof(SpGistMetaPageData)) - (char *) metapage;

            MarkBufferDirty(metabuffer);
            UnlockReleaseBuffer(metabuffer);
        } else {
            // Buffer locked by another process, skip update
            ReleaseBuffer(metabuffer);
        }
    }
}
```