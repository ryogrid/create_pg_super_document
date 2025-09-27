# XLogRecordAssemble

## Location
[src/backend/access/transam/xloginsert.c:548-943](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xloginsert.c#L548-L943)

## Overview
XLogRecordAssemble constructs a complete WAL record from all registered data and buffer references, preparing it for insertion into the WAL.

## Definition
static XLogRecData *XLogRecordAssemble(RmgrId rmid, uint8 info, XLogRecPtr RedoRecPtr, bool doPageWrites, XLogRecPtr *fpw_lsn, int *num_fpi, bool *topxid_included)

## Detailed Description
XLogRecordAssemble is the core function that assembles all components of a WAL record into a single XLogRecData chain. It processes registered buffers to determine which need full-page images, applies WAL compression when enabled, handles page hole optimization, includes replication origin and transaction ID information when needed, calculates CRC32C checksums, and enforces record size limits.

The function creates a structured record with header, block references with optional full-page images, optional metadata, and main data. It can be called multiple times for the same record and handles this properly.

## Parameters / Member Variables
- rmid: Resource Manager ID for the record type
- info: Info byte with operation flags and consistency checks
- RedoRecPtr: Current redo pointer for full-page write decisions
- doPageWrites: Whether full-page writes are enabled
- fpw_lsn: Output - lowest LSN of pages needing full-page images
- num_fpi: Output - count of full-page images included
- topxid_included: Output - whether top-level XID was logged

## Dependencies
- Functions called/Symbols referenced:
  - [PageGetLSN](../P/PageGetLSN.md), XLogCompressBackupBlock, GetTopTransactionIdIfAny
  - INIT_CRC32C, COMP_CRC32C, RelFileLocatorEquals
- Called from:
  - [XLogInsert](XLogInsert.md) (main insertion pathway)

## Notes and Other Information
- Static internal function not exposed externally
- Supports WAL compression (PGLZ, LZ4, ZSTD)
- Implements page hole optimization for standard pages
- Enforces XLogRecordMaxSize limits
- Handles consistency checking requirements
- Returns XLogRecData chain ready for insertion

## Simplified Source

```c
// Simplified version of XLogRecordAssemble
static XLogRecData *XLogRecordAssemble(RmgrId rmid, uint8 info,
                                      XLogRecPtr RedoRecPtr, bool doPageWrites,
                                      XLogRecPtr *fpw_lsn, int *num_fpi, bool *topxid_included) {
    XLogRecData *rdt;
    uint64 total_len = 0;
    int block_id;
    pg_crc32c rdata_crc;
    XLogRecData *rdt_datas_last;
    XLogRecord *rechdr;
    char *scratch = hdr_scratch;

    // Initialize record header
    rechdr = (XLogRecord *) scratch;
    scratch += SizeOfXLogRecord;
    hdr_rdt.next = NULL;
    rdt_datas_last = &hdr_rdt;
    hdr_rdt.data = hdr_scratch;

    // Enable consistency checking if requested
    if (wal_consistency_checking[rmid])
        info |= XLR_CHECK_CONSISTENCY;

    // Process all registered block references
    *fpw_lsn = InvalidXLogRecPtr;
    *num_fpi = 0;
    for (block_id = 0; block_id < max_registered_block_id; block_id++) {
        registered_buffer *regbuf = &registered_buffers[block_id];
        bool needs_backup;
        bool needs_data;
        XLogRecordBlockHeader bkpb;
        XLogRecordBlockImageHeader bimg;
        bool include_image;

        if (!regbuf->in_use)
            continue;

        // Determine if this block needs full-page backup
        if (regbuf->flags & REGBUF_FORCE_IMAGE)
            needs_backup = true;
        else if (regbuf->flags & REGBUF_NO_IMAGE)
            needs_backup = false;
        else if (!doPageWrites)
            needs_backup = false;
        else {
            XLogRecPtr page_lsn = PageGetLSN(regbuf->page);
            needs_backup = (page_lsn <= RedoRecPtr);
            if (!needs_backup) {
                if (*fpw_lsn == InvalidXLogRecPtr || page_lsn < *fpw_lsn)
                    *fpw_lsn = page_lsn;
            }
        }

        // Determine if buffer data is needed
        needs_data = (regbuf->rdata_len > 0) &&
                    ((regbuf->flags & REGBUF_KEEP_DATA) || !needs_backup);

        // Set up block header
        bkpb.id = block_id;
        bkpb.fork_flags = regbuf->forkno;
        bkpb.data_length = 0;

        if (regbuf->flags & REGBUF_WILL_INIT)
            bkpb.fork_flags |= BKPBLOCK_WILL_INIT;

        // Include full-page image if needed or for consistency checking
        include_image = needs_backup || (info & XLR_CHECK_CONSISTENCY) != 0;

        if (include_image) {
            Page page = regbuf->page;
            uint16 compressed_len = 0;
            bool is_compressed = false;

            // Calculate hole for standard pages
            if (regbuf->flags & REGBUF_STANDARD) {
                uint16 lower = ((PageHeader) page)->pd_lower;
                uint16 upper = ((PageHeader) page)->pd_upper;
                if (lower >= SizeOfPageHeaderData && upper > lower && upper <= BLCKSZ) {
                    bimg.hole_offset = lower;
                    // Simplified: assume no compression for clarity
                    bimg.length = BLCKSZ - (upper - lower);
                } else {
                    bimg.hole_offset = 0;
                    bimg.length = BLCKSZ;
                }
            } else {
                bimg.hole_offset = 0;
                bimg.length = BLCKSZ;
            }

            bkpb.fork_flags |= BKPBLOCK_HAS_IMAGE;
            (*num_fpi)++;

            // Set up image data chain
            rdt_datas_last->next = &regbuf->bkp_rdatas[0];
            rdt_datas_last = rdt_datas_last->next;

            bimg.bimg_info = (bimg.length == BLCKSZ) ? 0 : BKPIMAGE_HAS_HOLE;
            if (needs_backup)
                bimg.bimg_info |= BKPIMAGE_APPLY;

            // Add image data (simplified: no compression)
            rdt_datas_last->data = page;
            rdt_datas_last->len = bimg.length;
            total_len += bimg.length;
        }

        if (needs_data) {
            bkpb.fork_flags |= BKPBLOCK_HAS_DATA;
            bkpb.data_length = (uint16) regbuf->rdata_len;
            total_len += regbuf->rdata_len;

            rdt_datas_last->next = regbuf->rdata_head;
            rdt_datas_last = regbuf->rdata_tail;
        }

        // Copy headers to scratch buffer
        memcpy(scratch, &bkpb, SizeOfXLogRecordBlockHeader);
        scratch += SizeOfXLogRecordBlockHeader;
        if (include_image) {
            memcpy(scratch, &bimg, SizeOfXLogRecordBlockImageHeader);
            scratch += SizeOfXLogRecordBlockImageHeader;
        }
        memcpy(scratch, &regbuf->rlocator, sizeof(RelFileLocator));
        scratch += sizeof(RelFileLocator);
        memcpy(scratch, &regbuf->block, sizeof(BlockNumber));
        scratch += sizeof(BlockNumber);
    }

    // Add replication origin if needed
    if ((curinsert_flags & XLOG_INCLUDE_ORIGIN) &&
        replorigin_session_origin != InvalidRepOriginId) {
        *(scratch++) = (char) XLR_BLOCK_ID_ORIGIN;
        memcpy(scratch, &replorigin_session_origin, sizeof(replorigin_session_origin));
        scratch += sizeof(replorigin_session_origin);
    }

    // Add top-level XID if needed
    if (IsSubxactTopXidLogPending()) {
        TransactionId xid = GetTopTransactionIdIfAny();
        *topxid_included = true;
        *(scratch++) = (char) XLR_BLOCK_ID_TOPLEVEL_XID;
        memcpy(scratch, &xid, sizeof(TransactionId));
        scratch += sizeof(TransactionId);
    }

    // Add main data if present
    if (mainrdata_len > 0) {
        if (mainrdata_len > 255) {
            *(scratch++) = (char) XLR_BLOCK_ID_DATA_LONG;
            uint32 mainrdata_len_4b = (uint32) mainrdata_len;
            memcpy(scratch, &mainrdata_len_4b, sizeof(uint32));
            scratch += sizeof(uint32);
        } else {
            *(scratch++) = (char) XLR_BLOCK_ID_DATA_SHORT;
            *(scratch++) = (uint8) mainrdata_len;
        }
        rdt_datas_last->next = mainrdata_head;
        rdt_datas_last = mainrdata_last;
        total_len += mainrdata_len;
    }
    rdt_datas_last->next = NULL;

    hdr_rdt.len = (scratch - hdr_scratch);
    total_len += hdr_rdt.len;

    // Calculate CRC
    INIT_CRC32C(rdata_crc);
    COMP_CRC32C(rdata_crc, hdr_scratch + SizeOfXLogRecord, hdr_rdt.len - SizeOfXLogRecord);
    for (rdt = hdr_rdt.next; rdt != NULL; rdt = rdt->next)
        COMP_CRC32C(rdata_crc, rdt->data, rdt->len);

    // Check record size limit
    if (total_len > XLogRecordMaxSize)
        ereport(ERROR, (errmsg_internal("oversized WAL record")));

    // Fill in record header
    rechdr->xl_xid = GetCurrentTransactionIdIfAny();
    rechdr->xl_tot_len = (uint32) total_len;
    rechdr->xl_info = info;
    rechdr->xl_rmid = rmid;
    rechdr->xl_prev = InvalidXLogRecPtr;
    rechdr->xl_crc = rdata_crc;

    return &hdr_rdt;
}
```

Key simplifications made:
- Preserved the essential algorithm: process blocks → determine full-page images → add metadata → calculate CRC
- Removed complex compression logic for clarity while maintaining structure
- Simplified page hole calculation but preserved the optimization concept
- Focused on the core full-page write decision logic
- Maintained all essential components: block references, replication origin, transaction ID, main data
- Preserved critical size checking and CRC calculation