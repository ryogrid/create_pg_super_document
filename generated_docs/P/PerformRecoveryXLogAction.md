# PerformRecoveryXLogAction

## Location
[src/backend/access/transam/xlog.c:6263-6312](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L6263-L6312)

## Overview
PerformRecoveryXLogAction performs necessary WAL actions at the end of recovery to ensure crash recoverability, either by creating an end-of-recovery record during promotion or requesting a shutdown checkpoint.

## Definition

```c
static bool
PerformRecoveryXLogAction(void)
```
## Detailed Description
PerformRecoveryXLogAction is called at the conclusion of WAL recovery to perform final actions that ensure the database will be recoverable if it crashes again immediately after recovery completes. The function handles two distinct scenarios based on whether the system is being promoted from standby to primary:

**Promotion Scenario (Archive Recovery + Postmaster + Triggered Promotion):**
- Creates a lightweight end-of-recovery WAL record instead of a full checkpoint
- This allows the system to start accepting queries immediately while deferring the expensive checkpoint operation
- The checkpointer process may continue with an in-progress restartpoint, which is acceptable

**Normal Recovery Completion:**
- Requests a shutdown checkpoint with specific flags (CHECKPOINT_END_OF_RECOVERY, CHECKPOINT_IMMEDIATE, CHECKPOINT_WAIT)
- Uses shutdown checkpoint rather than online checkpoint to maintain the rule that timeline changes only occur in shutdown checkpoints
- This provides additional error checking capabilities in xlog_redo

The function returns a boolean indicating whether promotion occurred, which affects subsequent recovery completion logic.

## Parameters / Member Variables
- Returns:  - true if promotion was triggered, false otherwise

## Dependencies
- Functions called/Symbols referenced:
  - [PromoteIsTriggered](PromoteIsTriggered.md) (checks if standby promotion is in progress)
  - [CreateEndOfRecoveryRecord](../C/CreateEndOfRecoveryRecord.md) (writes end-of-recovery WAL record)
  - [RequestCheckpoint](../R/RequestCheckpoint.md) (initiates checkpoint process)
  - CHECKPOINT_END_OF_RECOVERY, CHECKPOINT_IMMEDIATE, CHECKPOINT_WAIT (checkpoint flags)
- Called from (representative examples):
  - [StartupXLOG](../S/StartupXLOG.md) (during recovery completion in startup process)
  - RefreshXLogWriteResult (in certain recovery contexts)

## Notes and Other Information
- Static function internal to xlog.c module
- Critical for ensuring crash recoverability immediately after recovery completion
- Handles the trade-off between quick startup (promotion) vs. full consistency (checkpoint)
- Maintains timeline ID change discipline by using shutdown checkpoints when assigning new timeline IDs
- The promotion path optimizes for faster failover by deferring expensive checkpoint operations
- In promotion scenarios, a checkpoint is requested later for safety after the system is fully operational
- Located in src/backend/access/transam/xlog.c:6263-6312