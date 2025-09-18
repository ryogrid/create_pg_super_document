# RemoveSocketFiles

## Location
src/backend/libpq/pqcomm.c: 847 - 879

## Overview
Removes all PostgreSQL socket files from the filesystem during postmaster shutdown to clean up resources.

## Definition


## Detailed Description
This function is responsible for cleaning up socket files when the PostgreSQL postmaster is shutting down. It iterates through the global list of socket file paths and removes each file from the filesystem using the unlink() system call.

The function is designed to be called during shutdown sequences and deliberately ignores any errors that occur during file removal, as the process is about to exit anyway. After attempting to remove all socket files, it sets the global sock_paths list to NIL, though the comment notes that storage reclamation isn't necessary since the process is exiting.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - ListCell (PostgreSQL list cell type)
  - foreach (PostgreSQL list iteration macro)
  - lfirst (PostgreSQL list access macro)
  - unlink (system call to remove files)
  - NIL (PostgreSQL constant for empty list)
  - sock_paths (global list variable containing socket file paths)

- Called from (representative examples):
  - CloseServerPorts (during postmaster shutdown to clean up socket files)

## Notes and Other Information
- This function is part of the cleanup process during postmaster termination
- Errors from unlink() calls are deliberately ignored since the process is shutting down
- The sock_paths list is set to NIL after processing, though this is mainly for completeness as the process is exiting
- Essential for preventing leftover socket files in the filesystem after PostgreSQL shutdown
- Works in conjunction with TouchSocketFiles to manage socket file lifecycle