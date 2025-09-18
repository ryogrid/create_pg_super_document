# _WriteData

## Location
src/bin/pg_dump/pg_backup_custom.c: 312 - 328

## Overview
Handles the actual writing of data chunks to the archive stream, supporting both table data and large object (LO) data through the custom archive format's compression system.

## Definition


## Detailed Description
The  function is a mandatory component of the custom archive format that serves as the primary data writing interface. It is called by the archiver whenever the dumper calls , making it the central bottleneck through which all table data and large object data flows during the dump process.

This function acts as a thin wrapper around the compression system, delegating the actual writing to the configured compressor's  method. The function is designed to handle both table data (initiated by ) and large object data (initiated by ), relying on the format's state management to distinguish between the two contexts.

The function includes built-in error handling through the compressor's  method, which internally throws write errors when issues occur.

## Parameters / Member Variables
- : Archive handle containing the overall archive state and format configuration
- : Pointer to the data buffer to be written to the archive
- : Size of the data buffer in bytes

## Dependencies
- Functions called/Symbols referenced:
  -  - Compressor's data writing method that handles compression and actual I/O
- Data structures used:
  -  - Local context for custom format containing compression state
  -  - State structure for the active compression algorithm
- Called from:
  -  - Custom format initialization (function pointer assignment)
  -  - Directory format initialization
  -  - Null format initialization
  - Various dumper routines via function pointer

## Notes and Other Information
- This function is marked as mandatory in the pg_dump architecture
- Designed to work with both table data and large object data seamlessly
- The function performs a safety check () before attempting to write
- Error handling is delegated to the compressor's writeData implementation
- Part of the pluggable archive format system that enables different storage backends
- The actual compression and I/O operations are abstracted through the CompressorState interface
- Should only be called from within a DataDumper routine context