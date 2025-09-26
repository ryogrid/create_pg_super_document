# bbsink_cleanup

## Location
src/include/backup/basebackup_sink.h: 265 - 301

## Overview
Initiates cleanup and resource deallocation for a bbsink object, ensuring proper resource management regardless of whether the backup completed successfully or failed.

## Definition

```c
structors for various types of sinks. */
extern bbsink *bbsink_copystream_new(bool send_to_client);
```
## Detailed Description
This inline function handles the cleanup phase for base backup sink objects. It delegates to sink-specific implementations to release any resources that would not be automatically freed, such as open file handles, network connections, allocated buffers, or temporary files. The function is designed to be called in two scenarios: after successful backup completion (following bbsink_end_backup) or when a backup is aborted due to an error.

The cleanup mechanism ensures that resources are properly released regardless of how the backup process terminates, preventing resource leaks and maintaining system stability. Different sink implementations may perform various cleanup tasks such as closing compressed streams, finalizing temporary files, or releasing network resources.

## Parameters / Member Variables
- : Pointer to the bbsink object that should perform cleanup operations. Must not be NULL.

## Dependencies
- Functions called/Symbols referenced:
  - bbsink (struct type)
  - Assert (assertion macro)
  - sink->bbs_ops->cleanup (callback function)

- Called from (representative examples):
  - SendBaseBackup
  - bbsink_forward_cleanup

## Notes and Other Information
- This is an inline function defined in the header file for performance
- Called both on successful backup completion and error abort scenarios
- Critical for preventing resource leaks in long-running PostgreSQL processes
- The actual cleanup behavior depends on the specific sink implementation (file cleanup, network disconnection, memory deallocation, etc.)
- Part of the base backup infrastructure ensuring proper resource management throughout the backup lifecycle
- Should be the final operation performed on a bbsink object before its destruction
- Essential for maintaining system stability especially in error conditions or interrupted backups