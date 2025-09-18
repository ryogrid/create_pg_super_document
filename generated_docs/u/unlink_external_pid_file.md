# unlink_external_pid_file

## Location
src/backend/postmaster/postmaster.c: 1423 - 1434

## Overview
unlink_external_pid_file is an on_proc_exit callback function that removes the external PID file created by the postmaster when the --external-pid-file option is used.

## Definition


## Detailed Description
This simple cleanup function ensures that external PID files are properly removed when the postmaster terminates. External PID files are created when PostgreSQL is started with the --external-pid-file command-line option, which allows system administrators or service management tools to track the postmaster's process ID in a custom location outside the data directory.

The function performs a basic safety check to ensure the external_pid_file global variable is set before attempting to remove the file. If the variable is NULL (meaning no external PID file was specified), the function does nothing. Otherwise, it calls the standard Unix unlink() system call to remove the file from the filesystem.

This cleanup mechanism is essential for proper system integration, preventing accumulation of stale PID files that might confuse service management tools or monitoring systems.

## Parameters / Member Variables
- : Exit status code (standard on_proc_exit callback parameter, unused)
- : Datum argument (standard on_proc_exit callback parameter, unused)

## Dependencies
- Functions called/Symbols referenced:
  - unlink: System call to remove file from filesystem
- Called from (representative examples):
  - [PostmasterMain](../P/PostmasterMain.md): Registered as on_proc_exit callback at line 1289
  - Referenced in SIGKILL_CHILDREN_AFTER_SECS context for shutdown sequencing

## Notes and Other Information
- Only executed if external_pid_file global variable is non-NULL, indicating an external PID file was created
- Registered as an on_proc_exit callback to ensure cleanup happens even during abnormal termination
- No error handling for unlink() failure - this is intentional as the file removal is not critical for database operation
- The external PID file feature is commonly used by system service managers like systemd, SysV init scripts, or Docker containers
- Complements the main postmaster.pid lock file in the data directory but serves different purposes