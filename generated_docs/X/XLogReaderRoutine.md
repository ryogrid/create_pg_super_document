# XLogReaderRoutine

## Location
src/include/access/xlogreader.h: 72 - 115

## Overview
XLogReaderRoutine is a callback interface structure that defines the essential I/O operations required for WAL reading, providing customizable page reading and segment management functions.

## Definition


## Detailed Description
XLogReaderRoutine serves as an abstraction layer that allows the WAL reading infrastructure to be customized for different environments and use cases. By providing callback functions for the fundamental I/O operations (page reading, segment opening, and segment closing), this structure enables the same WAL reading code to work with different storage backends, replication scenarios, and recovery contexts. The callback-based design allows for implementation-specific optimizations while maintaining a consistent interface for WAL record processing.

## Parameters / Member Variables
- : Callback function (XLogPageReadCB) responsible for reading WAL pages from storage. Must read at least reqLen bytes starting at targetPagePtr, handle timeline validation, and set appropriate timeline information. Should block if necessary to wait for data availability and return the number of bytes read or -1 on failure.
- : Callback function (WALSegmentOpenCB) for opening WAL segment files. Must set the file descriptor in ->seg.ws_file and handle timeline resolution. Takes segment number and timeline pointer as parameters, allowing timeline adjustment during opening.
- : Callback function (WALSegmentCloseCB) for closing WAL segment files. Must set ->seg.ws_file to a negative value to indicate the segment is closed. Provides cleanup and resource management for opened segments.

## Dependencies
- Functions called/Symbols referenced:
  - XLogPageReadCB (function type for page reading)
  - WALSegmentOpenCB (function type for segment opening)  
  - WALSegmentCloseCB (function type for segment closing)
- Called from (representative examples):
  - [XLogReaderAllocate](XLogReaderAllocate.md) (reader allocation with custom routines)
  - StartupDecodingContext (logical replication startup)
  - CreateInitDecodingContext (initial decoding context creation)
  - CreateDecodingContext (general decoding context creation)
  - XL_ROUTINE (routine access macro)
  - [XLogReaderState](XLogReaderState.md) (embedded within reader state)

## Notes and Other Information
This callback structure enables PostgreSQL's WAL reading infrastructure to work across diverse scenarios including crash recovery, streaming replication, logical replication, backup tools, and WAL analysis utilities. Different implementations can optimize for their specific requirements - for example, streaming replication might implement non-blocking reads while recovery operations might prefer blocking behavior. The timeline management capabilities in the callbacks are crucial for handling complex recovery scenarios involving timeline switches during point-in-time recovery operations.