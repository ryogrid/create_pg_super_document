# get_tablespace_io_concurrency

## Location
src/backend/utils/cache/spccache.c: 215 - 228

## Overview
Returns the I/O concurrency setting for a specified tablespace, falling back to the global default when no tablespace-specific value is configured.

## Definition


## Detailed Description
This function provides access to tablespace-specific I/O concurrency configuration, which controls the level of parallelism for I/O operations on the given tablespace. The I/O concurrency setting influences how PostgreSQL schedules and batches I/O operations, particularly for operations like bitmap heap scans and prefetching.

The function retrieves the cached tablespace entry and examines its effective_io_concurrency option. If no tablespace-specific setting exists (options are NULL) or if the configured value is negative (indicating "use default"), it returns the global effective_io_concurrency configuration parameter.

Like other tablespace parameter functions, this is not transaction-locked, meaning the returned value may change during query execution if concurrent modifications occur to the tablespace configuration.

## Parameters / Member Variables
- : The OID of the tablespace for which to retrieve the I/O concurrency setting

## Dependencies
- Functions called/Symbols referenced:
  - get_tablespace: Retrieve cached tablespace entry for the given OID
- Global variables referenced:
  - effective_io_concurrency: Global default I/O concurrency setting
- Data structures used:
  - TableSpaceCacheEntry: Cache entry containing tablespace options
  - TableSpaceOpts: Structure containing effective_io_concurrency field
- Called from:
  - ExecInitBitmapHeapScan: Initialize bitmap heap scan with appropriate I/O concurrency
  - read_stream_begin_relation: Configure read-ahead streams for relation scanning

## Notes and Other Information
- This is a public function accessible throughout the PostgreSQL backend
- Return value is not transaction-locked and may change during query execution
- Negative values in tablespace options indicate "use global default"
- Essential for optimizing I/O performance in heterogeneous storage environments
- Different tablespaces may have different optimal concurrency levels based on underlying storage characteristics
- Used by the executor to configure parallel I/O operations appropriately
- Part of PostgreSQL's adaptive I/O system that can be tuned per tablespace
- The function assumes the cache entry will always be valid (no explicit null check on spc)