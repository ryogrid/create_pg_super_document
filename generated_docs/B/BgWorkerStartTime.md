# BgWorkerStartTime

## Location
[src/include/postmaster/bgworker.h:82-83](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/postmaster/bgworker.h#L82-L83)

## Overview
BgWorkerStartTime is an enumeration that defines the different points during PostgreSQL server startup when a background worker can be launched.

## Definition


## Detailed Description
The BgWorkerStartTime enumeration provides precise control over when background workers are started during the PostgreSQL startup sequence. This timing control is crucial for ensuring that workers are launched at appropriate moments when required subsystems are available and the database is in the correct state. Different worker types have different requirements - some need basic postmaster functionality, others require a consistent database state, and some need full recovery completion before they can operate safely.

## Parameters / Member Variables
- : Worker starts immediately when the postmaster process begins, before database recovery
- : Worker starts when the database reaches a consistent state during recovery
- : Worker starts only after database recovery is completely finished

## Dependencies
- Functions called/Symbols referenced:
  - (No direct references - this is an enum definition)
- Called from (representative examples):
  - [BackgroundWorker](BackgroundWorker.md) (as bgw_start_time field)
  - [bgworker_should_start_now](../b/bgworker_should_start_now.md)

## Notes and Other Information
The choice of start time significantly impacts worker behavior and capabilities. Workers starting at PostmasterStart have the most limited environment - they cannot access the database catalog or perform SQL operations. Workers starting at ConsistentState can read from the database but may still be in recovery mode. Workers starting at RecoveryFinished have full database functionality available. This enumeration is essential for coordinating background worker initialization with PostgreSQL's complex startup sequence, ensuring workers don't attempt operations before the necessary infrastructure is ready.