# XLogRecordAssemble

## Location
src/backend/access/transam/xloginsert.c: 548 - 943

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