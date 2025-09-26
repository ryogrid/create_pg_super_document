# XLogReaderRoutine

## Location
[src/include/access/xlogreader.h:72-115](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/xlogreader.h#L72-L115)

## Overview
XLogReaderRoutine is a callback interface structure that defines the essential I/O operations required for WAL reading, providing customizable page reading and segment management functions.

## Definition

```c
typedef struct XLogReaderRoutine
{
	/*
	 * Data input callback
	 *
	 * This callback shall read at least reqLen valid bytes of the xlog page
	 * starting at targetPagePtr, and store them in readBuf.  The callback
	 * shall return the number of bytes read (never more than XLOG_BLCKSZ), or
	 * -1 on failure.  The callback shall sleep, if necessary, to wait for the
	 * requested bytes to become available.  The callback will not be invoked
	 * again for the same page unless more than the returned number of bytes
	 * are needed.
	 *
	 * targetRecPtr is the position of the WAL record we're reading.  Usually
	 * it is equal to targetPagePtr + reqLen, but sometimes xlogreader needs
	 * to read and verify the page or segment header, before it reads the
	 * actual WAL record it's interested in.  In that case, targetRecPtr can
	 * be used to determine which timeline to read the page from.
	 *
	 * The callback shall set ->seg.ws_tli to the TLI of the file the page was
	 * read from.
	 */
	XLogPageReadCB page_read;

	/*
	 * Callback to open the specified WAL segment for reading.  ->seg.ws_file
	 * shall be set to the file descriptor of the opened segment.  In case of
	 * failure, an error shall be raised by the callback and it shall not
	 * return.
	 *
	 * "nextSegNo" is the number of the segment to be opened.
	 *
	 * "tli_p" is an input/output argument. WALRead() uses it to pass the
	 * timeline in which the new segment should be found, but the callback can
	 * use it to return the TLI that it actually opened.
	 */
	WALSegmentOpenCB segment_open;

	/*
	 * WAL segment close callback.  ->seg.ws_file shall be set to a negative
	 * number.
	 */
	WALSegmentCloseCB segment_close;
} XLogReaderRoutine;
```
## Detailed Description
XLogReaderRoutine serves as an abstraction layer that allows the WAL reading infrastructure to be customized for different environments and use cases. By providing callback functions for the fundamental I/O operations (page reading, segment opening, and segment closing), this structure enables the same WAL reading code to work with different storage backends, replication scenarios, and recovery contexts. The callback-based design allows for implementation-specific optimizations while maintaining a consistent interface for WAL record processing.

## Parameters / Member Variables
- `page_read`: Callback function (XLogPageReadCB) responsible for reading WAL pages from storage. Must read at least reqLen bytes starting at targetPagePtr, handle timeline validation, and set appropriate timeline information. Should block if necessary to wait for data availability and return the number of bytes read or -1 on failure.
- `segment_open`: Callback function (WALSegmentOpenCB) for opening WAL segment files. Must set the file descriptor in ->seg.ws_file and handle timeline resolution. Takes segment number and timeline pointer as parameters, allowing timeline adjustment during opening.
- `segment_close`: Callback function (WALSegmentCloseCB) for closing WAL segment files. Must set ->seg.ws_file to a negative value to indicate the segment is closed. Provides cleanup and resource management for opened segments.
## Dependencies
- Functions called/Symbols referenced:
  - XLogPageReadCB (function type for page reading)
  - WALSegmentOpenCB (function type for segment opening)  
  - WALSegmentCloseCB (function type for segment closing)
- Called from (representative examples):
  - [XLogReaderAllocate](XLogReaderAllocate.md) (reader allocation with custom routines)
  - [StartupDecodingContext](../S/StartupDecodingContext.md) (logical replication startup)
  - [CreateInitDecodingContext](../C/CreateInitDecodingContext.md) (initial decoding context creation)
  - [CreateDecodingContext](../C/CreateDecodingContext.md) (general decoding context creation)
  - XL_ROUTINE (routine access macro)
  - [XLogReaderState](XLogReaderState.md) (embedded within reader state)

## Notes and Other Information
This callback structure enables PostgreSQL's WAL reading infrastructure to work across diverse scenarios including crash recovery, streaming replication, logical replication, backup tools, and WAL analysis utilities. Different implementations can optimize for their specific requirements - for example, streaming replication might implement non-blocking reads while recovery operations might prefer blocking behavior. The timeline management capabilities in the callbacks are crucial for handling complex recovery scenarios involving timeline switches during point-in-time recovery operations.