# CommitTsParameterChange

## Location
[src/backend/access/transam/commit_ts.c:664-704](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/commit_ts.c#L664-L704)

## Overview
CommitTsParameterChange handles activation or deactivation of the commit timestamp subsystem during WAL recovery when processing XLOG_PARAMETER_CHANGE records from the primary server.

## Definition
```c
void CommitTsParameterChange(bool newvalue, bool oldvalue)
```

## Detailed Description
This function is specifically designed to handle changes to the track_commit_timestamp parameter during WAL replay on standby servers. When a standby server receives an XLOG_PARAMETER_CHANGE record indicating that the primary server has changed its commit timestamp tracking setting, this function ensures the standby server's commit timestamp subsystem state remains synchronized.

The function implements conditional activation/deactivation logic:
- If the primary enables commit timestamp tracking and the standby doesn't have it active, it activates the local subsystem
- If the primary disables commit timestamp tracking and the standby has it active, it deactivates the local subsystem  
- If the states already match, no action is taken

This synchronization is crucial for maintaining consistency between primary and standby servers and ensuring that WAL records involving commit timestamps can be properly replayed.

## Parameters / Member Variables
- `newvalue`: Boolean indicating the new commit timestamp tracking state from the primary server
- `oldvalue`: Boolean indicating the previous commit timestamp tracking state (currently unused in implementation but provided for potential future use)

## Dependencies
- Functions called/Symbols referenced:
  - [ActivateCommitTs](../A/ActivateCommitTs.md)
  - [DeactivateCommitTs](../D/DeactivateCommitTs.md)
  - commitTsShared (global shared memory structure)
- Called from (representative examples):
  - [xlog_redo](../x/xlog_redo.md)

## Notes and Other Information
- This function only runs during recovery processing, not during normal operation
- Uses unlocked reads of shared memory, which is safe since it only runs in the recovery process
- The oldvalue parameter is currently unused but maintained for interface consistency
- Critical for maintaining primary-standby consistency for commit timestamp functionality
- The function checks commitTsShared->commitTsActive to determine current activation state
- Declared in src/include/access/commit_ts.h for external access
- Part of the WAL replay infrastructure for configuration parameter changes

## Simplified Source

```c
void CommitTsParameterChange(bool newvalue, bool oldvalue)
{
    // Synchronize commit timestamp tracking state with primary during WAL replay

    if (newvalue)
    {
        // Primary enabled commit timestamp tracking
        if (!commitTsShared->commitTsActive)
            ActivateCommitTs();
    }
    else if (commitTsShared->commitTsActive)
    {
        // Primary disabled commit timestamp tracking
        DeactivateCommitTs();
    }
}
```