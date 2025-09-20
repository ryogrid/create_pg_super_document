# SyncRepInitConfig

## Location
[src/backend/replication/syncrep.c:445-473](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/syncrep.c#L445-L473)

## Overview
Initializes synchronous replication configuration for WAL sender processes by determining the standby priority and updating the WAL sender's state accordingly.

## Definition
```c
void SyncRepInitConfig(void)
```

## Detailed Description
This function is called during WAL sender initialization and after configuration changes (SIGHUP) to update the synchronous replication priority of the current WAL sender process. It determines whether the connected standby is configured as a synchronous standby and what priority it has in the synchronous replication configuration.

The function queries the current standby priority using SyncRepGetStandbyPriority() and compares it with the previously stored value. If the priority has changed, it updates the WAL sender's sync_standby_priority field under mutex protection to ensure thread safety. This priority information is used by the synchronous replication system to determine which standbys must acknowledge WAL records before commits can complete.

When a priority change is detected, the function logs a debug message indicating the new synchronous standby priority for the connected application. This helps with monitoring and debugging synchronous replication configuration changes.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [SyncRepGetStandbyPriority](SyncRepGetStandbyPriority.md) (determines standby's sync priority)
  - SpinLockAcquire (acquires WAL sender mutex)
  - SpinLockRelease (releases WAL sender mutex)
  - ereport (logs debug message)
  - DEBUG1 (log level constant)
- Called from (representative examples):
  - [StartReplication](StartReplication.md)
  - [StartLogicalReplication](StartLogicalReplication.md)  
  - [ProcessPendingWrites](../P/ProcessPendingWrites.md)
  - [WalSndWaitForWal](../W/WalSndWaitForWal.md)
  - [WalSndLoop](../W/WalSndLoop.md)

## Notes and Other Information
This function is part of the WAL sender side of synchronous replication and is called frequently during WAL sender operation to handle configuration changes. The mutex protection ensures that priority updates are atomic with respect to other WAL sender operations that may read the priority value. The priority determination is based on the synchronous_standby_names configuration parameter and the application_name of the connected standby.