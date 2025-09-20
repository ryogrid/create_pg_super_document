# update_checkpoint_display

## Location
[src/backend/access/transam/xlog.c:6801-6862](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L6801-L6862)

## Overview
Updates the process status display for a process running a checkpoint or restartpoint operation to provide visibility into critical system operations.

## Definition

```c
static void
update_checkpoint_display(int flags, bool restartpoint, bool reset)
```
## Detailed Description
The update_checkpoint_display function manages the process status display (ps display) for checkpoint and restartpoint operations. It selectively updates the display only for critical operations like end-of-recovery checkpoints and shutdown checkpoints/restartpoints, where visibility is important since pg_stat_activity may not be reliable. The function is designed to be safe for use within critical sections by avoiding memory allocations.

The function constructs descriptive messages that indicate the type of operation being performed (end-of-recovery, shutdown) and whether it's a checkpoint or restartpoint. When reset is true, it clears the display; otherwise, it sets an informative activity message.

## Parameters / Member Variables
- `flags`: Checkpoint flags indicating the type of checkpoint operation (CHECKPOINT_END_OF_RECOVERY, CHECKPOINT_IS_SHUTDOWN, etc.)
- `restartpoint`: Boolean indicating whether this is a restartpoint (true) or checkpoint (false)
- `reset`: Boolean flag to either clear the display (true) or set an activity message (false)

## Dependencies
- Functions called/Symbols referenced:
  - set_ps_display
  - CHECKPOINT_END_OF_RECOVERY (constant)
  - CHECKPOINT_IS_SHUTDOWN (constant)
- Called from (representative examples):
  - [CreateCheckPoint](../C/CreateCheckPoint.md)
  - [CreateRestartPoint](../C/CreateRestartPoint.md)

## Notes and Other Information
- The function only updates the display for end-of-recovery and shutdown operations, filtering out routine checkpoints to avoid unnecessary overhead
- Designed to be allocation-free for safe use in critical sections
- Provides human-readable status information when pg_stat_activity may not be accessible
- The activity message format follows the pattern: "performing [end-of-recovery ][shutdown ][checkpoint|restartpoint]"