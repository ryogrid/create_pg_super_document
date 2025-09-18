# CleanupBackend

## Location
src/backend/postmaster/postmaster.c: 2791 - 2874

## Overview
CleanupBackend handles the cleanup operations after a PostgreSQL backend process terminates, removing all local state associated with the backend and determining if the termination requires emergency actions.

## Definition


## Detailed Description
CleanupBackend is a critical function in PostgreSQL's postmaster process that manages the aftermath of backend process termination. The function first logs the child exit event and then analyzes the exit status to determine the appropriate response. For normal exits (status 0 or 1), it performs orderly cleanup by removing the backend from the active backend list, releasing child slots, and canceling any background worker notifications. However, if the backend dies with an abnormal exit status, it triggers HandleChildCrash to initiate emergency procedures that signal all other backends to terminate quickly. On Windows, the function includes special handling for ERROR_WAIT_NO_CHILDREN (128) which is treated as a non-fatal case due to mutex-related startup issues.

## Parameters / Member Variables
- : Process ID of the terminated backend process
- : Exit status code of the terminated backend, used to determine if cleanup was normal or if emergency procedures are needed

## Dependencies
- Functions called/Symbols referenced:
  - LogChildExit
  - EXIT_STATUS_0
  - EXIT_STATUS_1
  - HandleChildCrash
  - ReleasePostmasterChildSlot
  - ShmemBackendArrayRemove
  - BackgroundWorkerStopNotifications
  - dlist_foreach_modify
  - dlist_container
  - dlist_delete
- Called from (representative examples):
  - process_pm_child_exit

## Notes and Other Information
- This function is platform-specific, with special Windows handling for ERROR_WAIT_NO_CHILDREN
- The function distinguishes between normal exits (0, 1) and crash scenarios requiring emergency response
- It's closely related to CleanupBackgroundWorker and changes should be coordinated
- Uses doubly-linked list operations to manage the BackendList
- Critical for maintaining system stability during backend failures