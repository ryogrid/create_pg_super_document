# log_heap_update

## Location
[src/backend/access/heap/heapam.c:8816-9037](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L8816-L9037)

## Overview
Performs XLogInsert for a heap-update operation, creating comprehensive WAL records for tuple updates with optimizations for space efficiency and logical decoding support.

## Definition

```c
static XLogRecPtr
log_heap_update(Relation reln, Buffer oldbuf,
				Buffer newbuf, HeapTuple oldtup, HeapTuple newtup,
				HeapTuple old_key_tuple,
				bool all_visible_cleared, bool new_all_visible_cleared)
```
## Detailed Description
The  function creates WAL records for heap tuple update operations. It implements sophisticated optimizations to minimize WAL volume by detecting common prefixes and suffixes between old and new tuple versions when they reside on the same page. The function handles both regular updates and HOT (Heap-Only Tuple) updates, supports logical decoding requirements, and manages visibility map clearing flags.

Key optimizations include:
- Prefix/suffix compression when old and new tuples are on the same page
- Conditional full-page image generation based on buffer backup needs
- Special handling for logical replication requiring complete tuple data
- Page initialization detection for new pages with single tuples

## Parameters / Member Variables
- `reln`: The relation being updated
- `oldbuf`: Buffer containing the old tuple's page
- `newbuf`: Buffer containing the new tuple's page
- `oldtup`: The old tuple being updated
- `newtup`: The new tuple version
- `old_key_tuple`: The old key tuple for replica identity (nullable)
- `all_visible_cleared`: Whether the old page's all-visible flag was cleared
- `new_all_visible_cleared`: Whether the new page's all-visible flag was cleared
## Dependencies
- Functions called/Symbols referenced:
  - [xl_heap_update](../x/xl_heap_update.md) (WAL record structure)
  - [xl_heap_header](../x/xl_heap_header.md) (tuple header structure)
  - RelationIsLogicallyLogged
  - RelationNeedsWAL
  - HeapTupleIsHeapOnly
  - [XLogBeginInsert](../X/XLogBeginInsert.md)
  - [XLogRegisterBuffer](../X/XLogRegisterBuffer.md)
  - [XLogRegisterData](../X/XLogRegisterData.md)
  - [XLogRegisterBufData](../X/XLogRegisterBufData.md)
  - [XLogInsert](../X/XLogInsert.md)
  - [XLogCheckBufferNeedsBackup](../X/XLogCheckBufferNeedsBackup.md)
  - [XLogSetRecordFlags](../X/XLogSetRecordFlags.md)
  - [ItemPointerGetOffsetNumber](../I/ItemPointerGetOffsetNumber.md)
  - HeapTupleHeaderGetRawXmax
  - [compute_infobits](../c/compute_infobits.md)
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md)
  - XLOG_HEAP_UPDATE/XLOG_HEAP_HOT_UPDATE
  - Various XLH_UPDATE_* flags
- Called from:
  - [heap_update](../h/heap_update.md)

## Notes and Other Information
- The function is static and only used internally within heapam.c
- Implements prefix/suffix compression that requires at least 3 bytes savings to be worthwhile
- Handles logical decoding by including complete tuple data when wal_level='logical'
- Supports replica identity by logging old key tuples for REPLICA_IDENTITY_FULL
- Automatically detects page initialization scenarios for new single-tuple pages
- Sets XLOG_INCLUDE_ORIGIN flag for origin filtering efficiency in logical replication
- The caller must ensure buffers are already modified and marked dirty before calling
- Compression optimization only works when old and new tuples are on the same page to avoid corruption propagation
- Returns XLogRecPtr representing the LSN of the inserted WAL record

## Simplified Source

```c
static XLogRecPtr
log_heap_update(Relation reln, Buffer oldbuf, Buffer newbuf,
                HeapTuple oldtup, HeapTuple newtup, HeapTuple old_key_tuple,
                bool all_visible_cleared, bool new_all_visible_cleared)
{
    xl_heap_update xlrec;
    xl_heap_header xlhdr;
    uint8 info;
    uint16 prefix_suffix[2];
    uint16 prefixlen = 0, suffixlen = 0;
    XLogRecPtr recptr;
    Page page = BufferGetPage(newbuf);
    bool need_tuple_data = RelationIsLogicallyLogged(reln);
    bool init;
    int bufflags;

    Assert(RelationNeedsWAL(reln));

    XLogBeginInsert();

    // Determine record type (regular or HOT update)
    info = HeapTupleIsHeapOnly(newtup) ? XLOG_HEAP_HOT_UPDATE : XLOG_HEAP_UPDATE;

    // Optimize WAL size by finding common prefix/suffix on same page
    if (oldbuf == newbuf && !need_tuple_data && !XLogCheckBufferNeedsBackup(newbuf)) {
        char *oldp = (char *) oldtup->t_data + oldtup->t_data->t_hoff;
        char *newp = (char *) newtup->t_data + newtup->t_data->t_hoff;
        int oldlen = oldtup->t_len - oldtup->t_data->t_hoff;
        int newlen = newtup->t_len - newtup->t_data->t_hoff;

        // Find common prefix (must save at least 3 bytes)
        for (prefixlen = 0; prefixlen < Min(oldlen, newlen); prefixlen++) {
            if (newp[prefixlen] != oldp[prefixlen])
                break;
        }
        if (prefixlen < 3) prefixlen = 0;

        // Find common suffix (must save at least 3 bytes)
        for (suffixlen = 0; suffixlen < Min(oldlen, newlen) - prefixlen; suffixlen++) {
            if (newp[newlen - suffixlen - 1] != oldp[oldlen - suffixlen - 1])
                break;
        }
        if (suffixlen < 3) suffixlen = 0;
    }

    // Setup WAL record flags
    xlrec.flags = 0;
    if (all_visible_cleared) xlrec.flags |= XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED;
    if (new_all_visible_cleared) xlrec.flags |= XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED;
    if (prefixlen > 0) xlrec.flags |= XLH_UPDATE_PREFIX_FROM_OLD;
    if (suffixlen > 0) xlrec.flags |= XLH_UPDATE_SUFFIX_FROM_OLD;
    if (need_tuple_data) {
        xlrec.flags |= XLH_UPDATE_CONTAINS_NEW_TUPLE;
        if (old_key_tuple) {
            xlrec.flags |= (reln->rd_rel->relreplident == REPLICA_IDENTITY_FULL) ?
                          XLH_UPDATE_CONTAINS_OLD_TUPLE : XLH_UPDATE_CONTAINS_OLD_KEY;
        }
    }

    // Check for page initialization
    init = (ItemPointerGetOffsetNumber(&(newtup->t_self)) == FirstOffsetNumber &&
            PageGetMaxOffsetNumber(page) == FirstOffsetNumber);
    if (init) info |= XLOG_HEAP_INIT_PAGE;

    // Setup record data
    xlrec.old_offnum = ItemPointerGetOffsetNumber(&oldtup->t_self);
    xlrec.old_xmax = HeapTupleHeaderGetRawXmax(oldtup->t_data);
    xlrec.old_infobits_set = compute_infobits(oldtup->t_data->t_infomask,
                                             oldtup->t_data->t_infomask2);
    xlrec.new_offnum = ItemPointerGetOffsetNumber(&newtup->t_self);
    xlrec.new_xmax = HeapTupleHeaderGetRawXmax(newtup->t_data);

    // Register buffers and data
    bufflags = REGBUF_STANDARD;
    if (init) bufflags |= REGBUF_WILL_INIT;
    if (need_tuple_data) bufflags |= REGBUF_KEEP_DATA;

    XLogRegisterBuffer(0, newbuf, bufflags);
    if (oldbuf != newbuf)
        XLogRegisterBuffer(1, oldbuf, REGBUF_STANDARD);

    XLogRegisterData((char *) &xlrec, SizeOfHeapUpdate);

    // Register prefix/suffix optimization data
    if (prefixlen > 0 || suffixlen > 0) {
        if (prefixlen > 0 && suffixlen > 0) {
            prefix_suffix[0] = prefixlen;
            prefix_suffix[1] = suffixlen;
            XLogRegisterBufData(0, (char *) &prefix_suffix, sizeof(uint16) * 2);
        } else if (prefixlen > 0) {
            XLogRegisterBufData(0, (char *) &prefixlen, sizeof(uint16));
        } else {
            XLogRegisterBufData(0, (char *) &suffixlen, sizeof(uint16));
        }
    }

    // Register new tuple data
    xlhdr.t_infomask2 = newtup->t_data->t_infomask2;
    xlhdr.t_infomask = newtup->t_data->t_infomask;
    xlhdr.t_hoff = newtup->t_data->t_hoff;

    XLogRegisterBufData(0, (char *) &xlhdr, SizeOfHeapHeader);

    // Register tuple data, excluding common prefix/suffix
    if (prefixlen == 0) {
        XLogRegisterBufData(0,
                           ((char *) newtup->t_data) + SizeofHeapTupleHeader,
                           newtup->t_len - SizeofHeapTupleHeader - suffixlen);
    } else {
        // Register bitmap and data separately when using prefix compression
        if (newtup->t_data->t_hoff - SizeofHeapTupleHeader > 0) {
            XLogRegisterBufData(0,
                               ((char *) newtup->t_data) + SizeofHeapTupleHeader,
                               newtup->t_data->t_hoff - SizeofHeapTupleHeader);
        }
        XLogRegisterBufData(0,
                           ((char *) newtup->t_data) + newtup->t_data->t_hoff + prefixlen,
                           newtup->t_len - newtup->t_data->t_hoff - prefixlen - suffixlen);
    }

    // Register old key tuple for logical replication if needed
    if (need_tuple_data && old_key_tuple) {
        xl_heap_header xlhdr_idx;
        xlhdr_idx.t_infomask2 = old_key_tuple->t_data->t_infomask2;
        xlhdr_idx.t_infomask = old_key_tuple->t_data->t_infomask;
        xlhdr_idx.t_hoff = old_key_tuple->t_data->t_hoff;

        XLogRegisterData((char *) &xlhdr_idx, SizeOfHeapHeader);
        XLogRegisterData((char *) old_key_tuple->t_data + SizeofHeapTupleHeader,
                        old_key_tuple->t_len - SizeofHeapTupleHeader);
    }

    XLogSetRecordFlags(XLOG_INCLUDE_ORIGIN);
    recptr = XLogInsert(RM_HEAP_ID, info);

    return recptr;
}
```