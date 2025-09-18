# CopyMethod

## Location
src/bin/pg_combinebackup/copy_file.h: 29 - 35

## Overview
CopyMethod is an enumeration that defines the different file copying strategies available in PostgreSQL's pg_combinebackup utility, allowing selection of optimal copy mechanisms based on platform capabilities and performance requirements.

## Definition
```c
typedef enum CopyMethod
{
    COPY_METHOD_CLONE,
    COPY_METHOD_COPY,
    COPY_METHOD_COPY_FILE_RANGE,
#ifdef WIN32
    COPY_METHOD_COPYFILE,
#endif
} CopyMethod;
```

## Detailed Description
CopyMethod is a platform-aware enumeration used by the pg_combinebackup utility to specify how files should be copied during backup combination operations. The enum provides multiple copy strategies, each optimized for different scenarios and platform capabilities:

- **COPY_METHOD_CLONE**: Uses filesystem-level cloning (like copy-on-write) for efficient copying when supported
- **COPY_METHOD_COPY**: Standard block-by-block copying method, universally supported
- **COPY_METHOD_COPY_FILE_RANGE**: Uses the copy_file_range system call on Linux for kernel-space copying
- **COPY_METHOD_COPYFILE**: Windows-specific copying using the CopyFile API (Windows only)

The enumeration is designed to enable performance optimization by leveraging platform-specific efficient copy mechanisms while maintaining portability across different operating systems.

## Parameters / Member Variables
- `COPY_METHOD_CLONE`: Filesystem-level cloning strategy for copy-on-write operations
- `COPY_METHOD_COPY`: Traditional block-by-block copy method
- `COPY_METHOD_COPY_FILE_RANGE`: Linux copy_file_range system call for efficient kernel-space copying
- `COPY_METHOD_COPYFILE`: Windows CopyFile API (conditionally compiled for Windows platforms)

## Dependencies
- Functions called/Symbols referenced:
  - Used as parameter type in copy_file function
- Called from (representative examples):
  - copy_file (src/bin/pg_combinebackup/copy_file.c:51)
  - cb_options (src/bin/pg_combinebackup/pg_combinebackup.c:80)
  - reconstruct_from_incremental_file (src/bin/pg_combinebackup/reconstruct.c:99)
  - write_reconstructed_file (src/bin/pg_combinebackup/reconstruct.c:557)

## Notes and Other Information
- On Windows platforms, the copy method is automatically overridden to COPY_METHOD_COPYFILE regardless of user selection
- The default copy method in pg_combinebackup is COPY_METHOD_COPY for maximum compatibility
- Command-line options in pg_combinebackup allow users to select specific copy methods: --clone, --copy, --copy-file-range
- The enumeration is part of the pg_combinebackup utility's file handling system, not part of the main PostgreSQL server
- Platform-specific compilation ensures only supported copy methods are available on each operating system