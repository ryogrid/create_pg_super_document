# heap_fetch_toast_slice

## Location
src/backend/access/heap/heaptoast.c: 626 - 793

## Overview
Fetches a specified slice (byte range) from a TOAST value stored in chunks across a heap table, assembling the requested portion into a result buffer.

## Definition


## Detailed Description
This function efficiently retrieves a contiguous byte slice from a large TOAST (The Oversized-Attribute Storage Technique) value without having to fetch the entire value. TOAST values are stored as multiple chunks in a separate toast table, and this function calculates which chunks contain the requested slice, fetches only those chunks, and copies the relevant portions to the result buffer.

The function performs the following key operations:
1. Opens indexes on the toast relation for efficient chunk lookup
2. Calculates which chunks (startchunk to endchunk) contain the requested slice
3. Sets up scan keys to query the toast index by valueid and chunk sequence numbers
4. Scans chunks in order, validating chunk sequence numbers and sizes
5. Copies only the relevant byte ranges from each chunk to the result buffer
6. Performs extensive error checking for data corruption and missing chunks

## Parameters / Member Variables
- : The relation (table) containing the TOAST chunks to be fetched from
- : Object identifier that uniquely identifies the specific TOAST value
- : Total size in bytes of the complete TOAST value (all chunks combined)
- : Starting byte offset within the TOAST value from which to begin fetching
- : Number of bytes to fetch from the TOAST value starting at sliceoffset
- : Pre-allocated varlena structure where the fetched slice data will be written

## Dependencies
- Functions called/Symbols referenced:
  - [toast_open_indexes](../t/toast_open_indexes.md)
  - [init_toast_snapshot](../i/init_toast_snapshot.md)
  - [systable_beginscan_ordered](../s/systable_beginscan_ordered.md)
  - [systable_getnext_ordered](../s/systable_getnext_ordered.md)
  - [systable_endscan_ordered](../s/systable_endscan_ordered.md)
  - [toast_close_indexes](../t/toast_close_indexes.md)
  - [fastgetattr](../f/fastgetattr.md)
  - [DatumGetInt32](../D/DatumGetInt32.md)
  - VARATT_IS_EXTENDED
  - VARATT_IS_SHORT
  - VARSIZE/VARSIZE_SHORT
  - VARDATA/VARDATA_SHORT
- Called from (representative examples):
  - [SampleHeapTupleVisible](../S/SampleHeapTupleVisible.md) (sampling functionality)
  - Functions working with TOAST_MAX_CHUNK_SIZE

## Notes and Other Information
- Uses TOAST_MAX_CHUNK_SIZE (typically 1996 bytes) as the standard chunk size for calculations
- Implements comprehensive error checking including validation of chunk sequence numbers, sizes, and completeness
- Optimizes scan keys based on slice requirements: single chunk gets equality condition, multiple chunks use range conditions
- Handles both regular and short varlena headers in toast chunks
- Uses ordered system table scans to ensure chunks are processed in sequence
- Provides detailed error messages with corruption details for debugging toast table issues
- Critical for PostgreSQL's ability to efficiently work with large column values without reading entire objects