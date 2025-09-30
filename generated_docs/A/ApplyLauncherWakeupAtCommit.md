# ApplyLauncherWakeupAtCommit

## Location
[src/backend/replication/logical/launcher.c:1118-1124](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/launcher.c#L1118-L1124)

## Overview
Requests the logical replication launcher to wake up upon commit of the current transaction, ensuring subscription processing occurs after catalog changes are committed.

## Definition

```c
void
ApplyLauncherWakeupAtCommit(void)
```
## Detailed Description
This function sets a flag () that signals the logical replication launcher to wake up from its sleep state when the current transaction commits. This mechanism is essential for maintaining consistency between catalog changes and subscription processing. The function uses a simple boolean flag to avoid redundant wake-up requests within the same transaction. The actual wakeup occurs during transaction commit processing, ensuring that subscription changes are only acted upon after they are durably committed to the database.

## Parameters / Member Variables
(This function takes no parameters)

## Dependencies
- Functions called/Symbols referenced:
  - on_commit_launcher_wakeup (static variable)
- Called from (representative examples):
  - [CreateSubscription](../C/CreateSubscription.md)
  - [AlterSubscription](AlterSubscription.md) 
  - [AlterSubscriptionOwner_internal](AlterSubscriptionOwner_internal.md)

## Notes and Other Information
- This function is typically called when new tuples are added to the pg_subscription catalog
- The function implements a simple optimization by only setting the flag once per transaction
- The actual launcher wakeup is deferred until transaction commit to ensure atomicity
- This is part of PostgreSQL's logical replication infrastructure for managing subscription workers

## Simplified Source

```c
void
ApplyLauncherWakeupAtCommit(void)
{
    // Set flag to wake up launcher when transaction commits
    // Avoids redundant wakeup requests within same transaction
    if (!on_commit_launcher_wakeup)
        on_commit_launcher_wakeup = true;
}
```