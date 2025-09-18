# CloseServerPorts

## Location
src/backend/postmaster/postmaster.c: 1389 - 1422

## Overview
CloseServerPorts is an on_proc_exit callback function that safely closes all server listening sockets and removes Unix socket files during postmaster shutdown to prevent race conditions with new postmaster instances.

## Definition


## Detailed Description
CloseServerPorts ensures orderly cleanup of network resources during postmaster termination. It performs a carefully sequenced shutdown process to avoid race conditions:

1. **Socket Closure**: Explicitly closes all listening socket file descriptors rather than relying on implicit closure at process exit. This prevents race conditions where a new postmaster might attempt to reuse the same TCP port number before the old sockets are fully closed by the system.

2. **Unix Socket Cleanup**: Removes filesystem entries for Unix domain sockets by calling RemoveSocketFiles(). This step occurs after socket closure but before lock file removal to maintain proper ordering.

3. **Lock File Handling**: Deliberately does not handle socket lock files, leaving that responsibility to later on_proc_exit callbacks to ensure proper cleanup sequencing.

The function is designed to be safe to call multiple times and handles errors gracefully by logging issues rather than failing catastrophically.

## Parameters / Member Variables
- : Exit status code (standard on_proc_exit callback parameter, unused)
- : Datum argument (standard on_proc_exit callback parameter, unused)

## Dependencies
- Functions called/Symbols referenced:
  - closesocket: Close individual socket file descriptors
  - [RemoveSocketFiles](../R/RemoveSocketFiles.md): Remove Unix domain socket files from filesystem
  - elog: Log error messages for failed socket closures
- Called from (representative examples):
  - [PostmasterMain](../P/PostmasterMain.md): Registered as on_proc_exit callback at line 1090
  - Referenced in SIGKILL_CHILDREN_AFTER_SECS context for shutdown sequencing

## Notes and Other Information
- Registered as an on_proc_exit callback during PostmasterMain initialization to ensure automatic cleanup
- Critical for preventing "Address already in use" errors when restarting PostgreSQL quickly
- Uses closesocket() rather than close() for cross-platform compatibility (Windows vs Unix)
- The ordering of operations (close sockets → remove socket files → remove lock files) is essential for race condition prevention
- Graceful error handling: socket closure failures are logged but don't prevent continuation of cleanup process
- NumListenSockets is reset to 0 after closing all sockets to prevent double-closure attempts