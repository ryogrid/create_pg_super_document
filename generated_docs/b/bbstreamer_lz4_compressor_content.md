# bbstreamer_lz4_compressor_content

## Location
src/bin/pg_basebackup/bbstreamer_lz4.c: 116 - 198

## Overview
Compresses input data using LZ4 compression and forwards it through the backup streaming chain.

## Definition


## Detailed Description
This function handles the core LZ4 compression operation for backup data streams. It manages the compression process by writing the LZ4 header on first invocation, calculating compression bounds to ensure sufficient output buffer capacity, and performing the actual data compression using LZ4F_compressUpdate.

The function implements a buffering strategy where it forwards compressed data to the next streamer when the output buffer approaches capacity limits. It dynamically resizes buffers when needed and maintains compression state across multiple invocations.

## Parameters / Member Variables
- : The LZ4 compressor streamer instance
- : Information about the current archive member being processed
- : Input data buffer to compress
- : Length of input data in bytes
- : Archive context information

## Dependencies
- Functions called/Symbols referenced:
  - LZ4F_compressBegin
  - LZ4F_compressUpdate  
  - LZ4F_compressBound
  - bbstreamer_content
  - enlargeStringInfo
- Called from (representative examples):
  - bbstreamer operation table (via function pointer)

## Notes and Other Information
- Writes LZ4 frame header before processing first data chunk
- Uses compression bounds calculation to prevent buffer overflows
- Forwards data to next streamer when output buffer capacity is insufficient
- Dynamically resizes output buffer if needed to accommodate compression bounds
- Part of the streaming compression pipeline for PostgreSQL backups