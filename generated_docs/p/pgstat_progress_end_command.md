# pgstat_progress_end_command

## Location
[src/backend/utils/activity/backend_progress.c:151-165](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/backend_progress.c#L151-L165)

## Overview
Resets the progress command tracking in the current backend's status entry, signaling the end of a long-running command that was being monitored.

## Definition

```c
void
pgstat_progress_end_command(void)
```
## Detailed Description
This function terminates progress tracking for the current backend by clearing the progress command indicators in the shared backend status structure. It sets  to  and  to , effectively signaling that no command is currently being tracked for progress reporting.

The function operates on the current backend's entry in the shared status array () and uses critical section macros to ensure atomic updates to the shared memory structure. If progress tracking is disabled () or no backend entry exists, the function returns early without making any changes.

The progress tracking mechanism allows PostgreSQL to report the status of long-running operations like VACUUM, REINDEX, CLUSTER, COPY, and others to monitoring tools and the  system views.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  -  (macro for starting critical section)
  -  (macro for ending critical section)
  -  (constant indicating no active command)
  -  (constant for invalid object identifier)
  -  (global variable pointing to current backend's status)
  -  (global flag controlling activity tracking)

- Called from (representative examples):
  -  (src/backend/access/heap/vacuumlazy.c:592)
  -  (src/backend/access/transam/xact.c:2776)
  -  (src/backend/access/transam/xact.c:5185)
  -  (src/backend/catalog/index.c:3848)
  -  (src/backend/commands/analyze.c:269)
  -  (src/backend/commands/cluster.c:342, 488)
  -  (src/backend/commands/copyfrom.c:1803)
  -  (src/backend/commands/indexcmds.c:1230, 1543, 1566, 1774)

## Notes and Other Information
- The function uses critical section macros (/) to ensure atomic updates to shared memory
- Early returns if  is NULL,  is disabled, or no progress command is currently active
- Automatically called during transaction abort to ensure progress tracking is properly cleaned up
- Part of PostgreSQL's statistics collection system that enables monitoring of long-running operations
- The function is location: src/backend/utils/activity/backend_progress.c:151-165

## Simplified Source

```c
// Simplified version of pgstat_progress_end_command
void pgstat_progress_end_command(void) {
    volatile PgBackendStatus *beentry = MyBEEntry;

    // Early exit if tracking is disabled or no backend entry
    if (!beentry || !pgstat_track_activities)
        return;

    // Early exit if no progress command is currently active
    if (beentry->st_progress_command == PROGRESS_COMMAND_INVALID)
        return;

    // Atomically clear progress tracking fields
    PGSTAT_BEGIN_WRITE_ACTIVITY(beentry);
    beentry->st_progress_command = PROGRESS_COMMAND_INVALID;
    beentry->st_progress_command_target = InvalidOid;
    PGSTAT_END_WRITE_ACTIVITY(beentry);
}
```

Key simplifications made:
- Added clear comments explaining each logical step
- Preserved all original logic and control flow
- Made the three main phases explicit: validation checks, active command check, and atomic cleanup
- No simplification of logic was needed as the function is already quite concise and focused