# copyFileByRange

## Location
src/bin/pg_upgrade/file.c: 151 - 189

## Overview
Efficiently copies a relation file from source to destination using the Linux copy_file_range system call for optimized kernel-space file copying.

## Definition


## Detailed Description
The copyFileByRange function implements efficient file copying using the Linux-specific  system call. This system call allows the kernel to copy data between file descriptors without transferring data to user space, potentially enabling optimizations such as copy-on-write or server-side copying for network filesystems.

The function opens both source and destination files, then uses a loop to copy the entire file content using  with maximum transfer size (). The copying continues until all data has been transferred (indicated by  returning 0).

This approach is more efficient than traditional read/write loops as it avoids multiple context switches between user and kernel space and allows the kernel to optimize the copying process.

## Parameters / Member Variables
- : Source file path to copy from
- : Destination file path to create and copy to
- : SQL schema name of the relation (used only for error reporting)
- : SQL relation name (used only for error reporting)

## Dependencies
- Functions called/Symbols referenced:
  - open
  - copy_file_range
  - close
  - [pg_fatal](../p/pg_fatal.md)
  - PG_BINARY
  - pg_file_create_mode
  - ssize_t
- Called from (representative examples):
  - [transfer_relfile](../t/transfer_relfile.md)

## Notes and Other Information
- The function is conditionally compiled and only available when  is defined
- Requires Linux kernel 4.5 or later for  system call support
- The function uses  as the maximum number of bytes to copy in each call, allowing the kernel to determine optimal transfer sizes
- Creates the destination file with exclusive creation flags () to prevent overwriting existing files
- Used as an alternative to  when file cloning is not available or appropriate
- Part of the pg_upgrade utility's file transfer mechanism for relation files during PostgreSQL upgrades
- Provides better performance than traditional userspace copying by reducing data copying between kernel and user space