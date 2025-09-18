# pgstat_fetch_stat_io

## Location
[src/backend/utils/activity/pgstat_io.c:157-172](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_io.c#L157-L172)

## Overview
Retrieves a snapshot of I/O statistics from PostgreSQL's shared statistics system.

## Definition


## Detailed Description
This function provides access to the current I/O statistics by taking a snapshot of the shared statistics data for I/O operations. It ensures that the statistics are consistent by calling  with the  kind, which captures the current state of I/O statistics from the shared memory into the local snapshot. The function then returns a pointer to the I/O statistics structure in the local snapshot.

## Parameters / Member Variables
(This function takes no parameters)

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_snapshot_fixed](pgstat_snapshot_fixed.md)
  - PGSTAT_KIND_IO
- Called from (representative examples):
  - [pg_stat_get_io](pg_stat_get_io.md)

## Notes and Other Information
- The function returns a pointer to the I/O statistics structure in the local snapshot ()
- The snapshot ensures consistency of the statistics data at the time of the call
- This is part of PostgreSQL's statistics collection system for monitoring I/O operations
- Located in src/backend/utils/activity/pgstat_io.c:157-172