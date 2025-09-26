# read_stream_begin_relation

## Location
src/backend/storage/aio/read_stream.c: 389 - 566

## Overview
Creates a new read stream object for efficiently reading blocks from a specific fork of a PostgreSQL relation using vectored I/O and prefetching strategies.

## Definition


## Detailed Description
This function initializes a read stream that optimizes sequential and random access patterns by performing lookahead and combining multiple block reads into larger vectored I/O operations. The read stream manages a queue of pinned buffers and uses callbacks to determine which blocks to read next. It automatically adjusts the number of concurrent I/Os based on tablespace configuration and system capabilities.

The function calculates optimal buffer queue sizes, I/O concurrency limits, and prefetch behavior based on the relation type, access flags, and system configuration. It supports both catalog relations (with conservative settings) and user relations (with configurable tablespace-specific settings).

## Parameters / Member Variables
- : Control flags including READ_STREAM_MAINTENANCE, READ_STREAM_SEQUENTIAL, and READ_STREAM_FULL
- : Buffer access strategy to control buffer replacement policy and pin limits
- : The relation to read from
- : Fork number (main, FSM, VM, etc.) of the relation to read
- : Function to call for determining the next block number to read
- : Private data passed to the callback function
- : Size of additional data to allocate per buffer for callback use

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetSmgr
  - IsCatalogRelation
  - IsCatalogRelationOid
  - get_tablespace_maintenance_io_concurrency
  - get_tablespace_io_concurrency
  - GetAccessStrategyPinLimit
  - SmgrIsTemp
  - LimitAdditionalLocalPins
  - LimitAdditionalPins
- Called from (representative examples):
  - heap_beginscan
  - acquire_sample_rows

## Notes and Other Information
- Automatically detects and handles catalog relations with conservative I/O settings to avoid circular dependencies
- Supports prefetch advice on systems with USE_PREFETCH enabled, except when direct I/O is active
- Allocates all required memory (buffers, I/O tracking, per-buffer data) in a single allocation for efficiency
- Queue size includes overflow space to handle multi-block I/Os that might extend beyond the regular queue boundary
- The distance parameter starts at 1 for gradual ramp-up or at the combine limit for full relation scans