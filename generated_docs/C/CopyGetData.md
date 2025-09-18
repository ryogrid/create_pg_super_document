# CopyGetData

## Location
src/backend/commands/copyfromparse.c: 245 - 361

## Overview
Reads raw data from various COPY sources (file, frontend, or callback) with support for minimum and maximum read requirements, handling different protocol messages and EOF conditions appropriately.

## Definition
static int CopyGetData(CopyFromState cstate, void *databuf, int minread, int maxread)

## Detailed Description
CopyGetData is the core data reading function for COPY operations that handles three different data sources. For COPY_FILE operations, it uses standard fread() and handles file I/O errors. For COPY_FRONTEND operations, it implements the PostgreSQL frontend/backend protocol by reading and processing various message types (CopyData, CopyDone, CopyFail, Flush, Sync), properly handling protocol violations and connection failures. For COPY_CALLBACK operations, it delegates to a user-provided callback function. The function attempts to read between minread and maxread bytes, returning the actual number of bytes read or indicating EOF conditions.

## Parameters / Member Variables
- `cstate`: CopyFromState structure containing the current state and configuration of the COPY operation, including the data source type and associated buffers
- `databuf`: Destination buffer where the read data will be stored  
- `minread`: Minimum number of bytes that should be read (if fewer bytes are available, EOF is assumed)
- `maxread`: Maximum number of bytes to read in this call

## Dependencies
- Functions called/Symbols referenced:
  - fread (for file-based copy operations)
  - ferror (file error checking)
  - pq_startmsgread, pq_getbyte, pq_getmessage (protocol message handling)
  - pq_copymsgbytes (copy data from message buffer)
  - pq_getmsgstring (extract string from message)
  - HOLD_CANCEL_INTERRUPTS/RESUME_CANCEL_INTERRUPTS (interrupt management)
  - ereport/errcode/errmsg (error reporting)
- Called from (representative examples):
  - CopyLoadRawBuf (src/backend/commands/copyfromparse.c:627)
  - CopyReadLine (src/backend/commands/copyfromparse.c:1122)

## Notes and Other Information
- No data conversion is applied - this function only handles raw data transfer
- For frontend sources, properly validates protocol messages and rejects invalid message types
- Handles connection failures by reporting errors rather than allowing incomplete operations
- Supports callback-based data sources for extensibility
- Sets raw_reached_eof flag when end-of-data conditions are detected
- Ignores Flush and Sync messages for client library compatibility