# dsm_cleanup_for_mmap

## Location
[src/backend/storage/ipc/dsm.c:320-357](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/dsm.c#L320-L357)

## Overview
Cleans up leftover memory-mapped files from previous PostgreSQL invocations by scanning and removing all mmap-based DSM segment files from the dynamic shared memory directory.

## Definition
```c
static void dsm_cleanup_for_mmap(void)
```

## Detailed Description
This function is specifically designed for the mmap implementation of dynamic shared memory, where DSM segments are backed by files in the filesystem. Unlike other shared memory implementations, mmap-based segments can survive operating system reboots, but their control segment may become out of date or corrupted. Rather than relying on potentially invalid control segment information, this function takes a direct approach by scanning the PG_DYNSHMEM_DIR directory and removing all files that match the expected naming pattern for DSM segments. This ensures a clean slate for the new postmaster process without depending on potentially stale metadata.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - [AllocateDir](../A/AllocateDir.md)
  - [ReadDir](../R/ReadDir.md)
  - [FreeDir](../F/FreeDir.md)
  - unlink
  - snprintf
  - elog
  - ereport
  - [errcode_for_file_access](../e/errcode_for_file_access.md)
- Called from (representative examples):
  - [dsm_postmaster_startup](dsm_postmaster_startup.md)

## Notes and Other Information
- This is a static function, only visible within the dsm.c compilation unit
- Only used when dynamic_shared_memory_type == DSM_IMPL_MMAP
- Scans files in PG_DYNSHMEM_DIR for the PG_DYNSHMEM_MMAP_FILE_PREFIX pattern
- More aggressive cleanup approach compared to control segment-based cleanup
- Necessary because mmap files can survive system reboots unlike other shared memory types
- Reports errors if file removal fails, ensuring cleanup doesn't silently fail
- Logs each file removal at DEBUG2 level for troubleshooting
- Called during postmaster startup before creating new control segment
- Essential for preventing accumulation of stale mmap files across restarts

## Simplified Source

```c
// Simplified version of dsm_cleanup_for_mmap
static void dsm_cleanup_for_mmap(void) {
    DIR *dir;
    struct dirent *dent;

    // Open the dynamic shared memory directory
    dir = AllocateDir(PG_DYNSHMEM_DIR);

    // Scan directory for mmap DSM files
    while ((dent = ReadDir(dir, PG_DYNSHMEM_DIR)) != NULL) {
        // Check if filename matches DSM mmap file pattern
        if (strncmp(dent->d_name, PG_DYNSHMEM_MMAP_FILE_PREFIX,
                    strlen(PG_DYNSHMEM_MMAP_FILE_PREFIX)) == 0) {
            char buf[MAXPGPATH + sizeof(PG_DYNSHMEM_DIR)];

            // Build full file path
            snprintf(buf, sizeof(buf), PG_DYNSHMEM_DIR "/%s", dent->d_name);

            // Log file removal for debugging
            elog(DEBUG2, "removing file \"%s\"", buf);

            // Remove the DSM file
            if (unlink(buf) != 0) {
                ereport(ERROR,
                        (errcode_for_file_access(),
                         errmsg("could not remove file \"%s\": %m", buf)));
            }
        }
    }

    // Clean up directory handle
    FreeDir(dir);
}
```

Key simplifications made:
- Preserved the essential directory scanning and file removal logic
- Kept critical error handling for file removal failures
- Maintained the pattern matching logic for DSM files
- Preserved logging for debugging purposes
- Focused on the main execution path without removing important functionality