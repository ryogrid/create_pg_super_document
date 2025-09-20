# autovac_init

## Location
[src/backend/postmaster/autovacuum.c:3287-3299](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/autovacuum.c#L3287-L3299)

## Overview
The autovac_init function performs initialization checks for the autovacuum subsystem at postmaster startup, validating configuration parameters and issuing warnings for misconfiguration.

## Definition

```c
struct and the array of WorkerInfoData.
	 */
	size = sizeof(AutoVacuumShmemStruct);
```
## Detailed Description
This function is called during postmaster initialization to validate autovacuum configuration. It performs a simple but critical check: if the autovacuum daemon is configured to start (autovacuum_start_daemon is true) but statistics tracking is disabled (pgstat_track_counts is false), it issues a warning message explaining that autovacuum cannot function properly without statistics tracking enabled.

The function serves as an early warning system to alert administrators about configuration issues that would prevent autovacuum from working effectively. Autovacuum relies on table usage statistics to determine when tables need vacuuming or analyzing, so without track_counts enabled, the daemon would be unable to make informed decisions about maintenance operations.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - ereport (for warning message reporting)
  - [errmsg](../e/errmsg.md) (error message formatting)
  - [errhint](../e/errhint.md) (hint message formatting)
- Global variables checked:
  - autovacuum_start_daemon (configuration variable)
  - pgstat_track_counts (configuration variable)
- Called from:
  - [PostmasterMain](../P/PostmasterMain.md) (at src/backend/postmaster/postmaster.c:1301)

## Notes and Other Information
- This function is part of the autovacuum subsystem initialization sequence
- The warning message helps administrators identify a common misconfiguration where autovacuum is enabled but statistics collection is disabled
- The function's comment humorously states its purpose as "annoy the user if he got it wrong", emphasizing its role as a configuration validator
- Located in src/backend/postmaster/autovacuum.c:3287-3299