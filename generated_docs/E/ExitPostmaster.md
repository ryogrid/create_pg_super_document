# ExitPostmaster

## Location
[src/backend/postmaster/postmaster.c:3669-3703](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/postmaster.c#L3669-L3703)

## Overview
ExitPostmaster is a static cleanup function that provides a controlled way to exit the PostgreSQL postmaster process, ensuring proper cleanup is performed before termination.

## Definition
static void ExitPostmaster(int status)

## Detailed Description
ExitPostmaster serves as the central exit point for the postmaster process in PostgreSQL. This function ensures that the postmaster never calls exit() directly, instead routing all exit requests through this controlled cleanup function. The function performs critical checks before termination, including verifying that the postmaster has not become multithreaded (which would indicate an abnormal state) and then delegates the actual exit process to proc_exit() which handles shared memory cleanup and backend process management.

The function includes a platform-specific check for multithreading on systems that support pthread_is_threaded_np(), reporting a LOG-level message if the postmaster has unexpectedly become multithreaded, which could indicate environment issues (particularly locale-related problems).

## Parameters / Member Variables
- status: Exit status code to be passed to the underlying proc_exit() function

## Dependencies
- Functions called/Symbols referenced:
  - pthread_is_threaded_np (conditional, platform-specific)
  - ereport (for logging warnings)
  - [proc_exit](../p/proc_exit.md) (for actual process termination)
- Called from (representative examples):
  - [PostmasterMain](../P/PostmasterMain.md) (multiple exit points)
  - [process_pm_child_exit](../p/process_pm_child_exit.md)
  - [PostmasterStateMachine](../P/PostmasterStateMachine.md)
  - [checkControlFile](../c/checkControlFile.md)
  - [StartChildProcess](../S/StartChildProcess.md)

## Notes and Other Information
- This function must be used instead of calling exit() directly to ensure proper postmaster cleanup
- Includes debugging comments from original developers (vadim 05-10-1999) about backend termination semantics
- The multithreading check is primarily defensive, as postmasters should not become multithreaded during normal operation
- Platform-specific behavior on systems with pthread_is_threaded_np() support

## Simplified Source

```c
// Simplified version of ExitPostmaster
static void ExitPostmaster(int status) {
    // Check for unexpected multithreading (platform-specific)
    #ifdef HAVE_PTHREAD_IS_THREADED_NP
    if (pthread_is_threaded_np() != 0) {
        ereport(LOG,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("postmaster became multithreaded"),
                 errhint("Set the LC_ALL environment variable to a valid locale.")));
    }
    #endif

    // Perform actual cleanup and exit
    proc_exit(status);
}
```

Key simplifications made:
- Removed verbose comments about cleanup semantics and historical notes
- Consolidated multithreading check into a single clear block
- Focused on the two core operations: safety check and exit delegation
- Preserved essential error reporting for the multithreading condition