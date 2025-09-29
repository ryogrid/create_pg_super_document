# XLogReaderValidatePageHeader

## Location
[src/backend/access/transam/xlogreader.c:1234-1374](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogreader.c#L1234-L1374)

## Overview
XLogReaderValidatePageHeader validates WAL page headers to ensure structural integrity and consistency before processing page contents.

## Definition
```c
bool XLogReaderValidatePageHeader(XLogReaderState *state, XLogRecPtr recptr, char *phdr)
```

## Detailed Description
XLogReaderValidatePageHeader performs comprehensive validation of WAL page headers to ensure data integrity and system consistency. The function executes multiple validation checks:

1. **Magic Number Validation**: Verifies the page header contains the correct XLOG_PAGE_MAGIC value
2. **Info Bits Validation**: Ensures only valid flag bits are set in xlp_info field
3. **Long Header Processing**: For pages with XLP_LONG_HEADER flag:
   - Validates system identifier matches the expected database system
   - Verifies segment size consistency
   - Checks XLOG_BLCKSZ consistency
4. **First Page Validation**: Ensures the first page of a WAL segment has a long header
5. **Page Address Validation**: Confirms the page address matches the expected location (detects recycled segments)
6. **Timeline Validation**: Ensures timeline IDs don't go backwards across successive pages

The function also updates the reader state by tracking the latest validated page pointer and timeline ID for future consistency checks.

## Parameters / Member Variables
- `state`: XLogReaderState pointer containing reader state, system configuration, and error reporting context
- `recptr`: XLogRecPtr specifying the expected page location in the WAL (must be page-aligned)
- `phdr`: Character pointer to the page header data to be validated

## Dependencies
- Functions called/Symbols referenced:
  - XLogSegNo, XLogPageHeader, XLogLongPageHeader (WAL data types)
  - XLByteToSeg (converts LSN to segment number)
  - XLogSegmentOffset (calculates offset within segment)
  - [XLogFileName](XLogFileName.md) (generates WAL filename for error reporting)
  - XLOG_PAGE_MAGIC (expected magic number constant)
  - XLP_ALL_FLAGS, XLP_LONG_HEADER (page header flag constants)
  - MAXFNAMELEN (maximum filename length constant)
  - [report_invalid_record](../r/report_invalid_record.md) (error reporting function)
- Called from (representative examples):
  - [ReadPageInternal](../R/ReadPageInternal.md) (at lines 1061 and 1103)
  - [XLogPageRead](XLogPageRead.md) (at line 3467 in xlogrecovery.c)
  - [XLogPageReadResult](XLogPageReadResult.md) (header function)

## Notes and Other Information
- Public function (non-static) available for use by other WAL processing components
- Assumes recptr is page-aligned (asserted with XLOG_BLCKSZ alignment check)
- Critical for detecting recycled WAL segments that haven't been overwritten yet
- Timeline validation prevents reading inconsistent WAL sequences
- Updates state->latestPagePtr and state->latestPageTLI to track progression
- Long headers are mandatory on the first page of each WAL segment
- Returns boolean indicating validation success/failure with detailed error reporting

## Simplified Source

```c
bool XLogReaderValidatePageHeader(XLogReaderState *state, XLogRecPtr recptr, char *phdr)
{
    XLogSegNo segno;
    int32 offset;
    XLogPageHeader hdr = (XLogPageHeader) phdr;

    Assert((recptr % XLOG_BLCKSZ) == 0);

    XLByteToSeg(recptr, segno, state->segcxt.ws_segsize);
    offset = XLogSegmentOffset(recptr, state->segcxt.ws_segsize);

    // Check magic number
    if (hdr->xlp_magic != XLOG_PAGE_MAGIC)
    {
        char fname[MAXFNAMELEN];
        XLogFileName(fname, state->seg.ws_tli, segno, state->segcxt.ws_segsize);
        report_invalid_record(state,
                            "invalid magic number %04X in WAL segment %s, LSN %X/%X, offset %u",
                            hdr->xlp_magic, fname, LSN_FORMAT_ARGS(recptr), offset);
        return false;
    }

    // Check info bits are valid
    if ((hdr->xlp_info & ~XLP_ALL_FLAGS) != 0)
    {
        char fname[MAXFNAMELEN];
        XLogFileName(fname, state->seg.ws_tli, segno, state->segcxt.ws_segsize);
        report_invalid_record(state,
                            "invalid info bits %04X in WAL segment %s, LSN %X/%X, offset %u",
                            hdr->xlp_info, fname, LSN_FORMAT_ARGS(recptr), offset);
        return false;
    }

    // Handle long header validation
    if (hdr->xlp_info & XLP_LONG_HEADER)
    {
        XLogLongPageHeader longhdr = (XLogLongPageHeader) hdr;

        // Check system identifier
        if (state->system_identifier && longhdr->xlp_sysid != state->system_identifier)
        {
            report_invalid_record(state,
                                "WAL file is from different database system: "
                                "WAL file database system identifier is %llu, "
                                "pg_control database system identifier is %llu",
                                (unsigned long long) longhdr->xlp_sysid,
                                (unsigned long long) state->system_identifier);
            return false;
        }

        // Check segment size
        if (longhdr->xlp_seg_size != state->segcxt.ws_segsize)
        {
            report_invalid_record(state,
                                "WAL file is from different database system: "
                                "incorrect segment size in page header");
            return false;
        }

        // Check block size
        if (longhdr->xlp_xlog_blcksz != XLOG_BLCKSZ)
        {
            report_invalid_record(state,
                                "WAL file is from different database system: "
                                "incorrect XLOG_BLCKSZ in page header");
            return false;
        }
    }
    else if (offset == 0)
    {
        // First page must have long header
        char fname[MAXFNAMELEN];
        XLogFileName(fname, state->seg.ws_tli, segno, state->segcxt.ws_segsize);
        report_invalid_record(state,
                            "invalid info bits %04X in WAL segment %s, LSN %X/%X, offset %u",
                            hdr->xlp_info, fname, LSN_FORMAT_ARGS(recptr), offset);
        return false;
    }

    // Check page address matches expected location
    if (hdr->xlp_pageaddr != recptr)
    {
        char fname[MAXFNAMELEN];
        XLogFileName(fname, state->seg.ws_tli, segno, state->segcxt.ws_segsize);
        report_invalid_record(state,
                            "unexpected pageaddr %X/%X in WAL segment %s, LSN %X/%X, offset %u",
                            LSN_FORMAT_ARGS(hdr->xlp_pageaddr), fname,
                            LSN_FORMAT_ARGS(recptr), offset);
        return false;
    }

    // Check timeline consistency (TLI should not go backwards)
    if (recptr > state->latestPagePtr)
    {
        if (hdr->xlp_tli < state->latestPageTLI)
        {
            char fname[MAXFNAMELEN];
            XLogFileName(fname, state->seg.ws_tli, segno, state->segcxt.ws_segsize);
            report_invalid_record(state,
                                "out-of-sequence timeline ID %u (after %u) in "
                                "WAL segment %s, LSN %X/%X, offset %u",
                                hdr->xlp_tli, state->latestPageTLI, fname,
                                LSN_FORMAT_ARGS(recptr), offset);
            return false;
        }
    }

    // Update state with latest valid page
    state->latestPagePtr = recptr;
    state->latestPageTLI = hdr->xlp_tli;

    return true;
}
```