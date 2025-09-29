# ResetUnloggedRelationsInDbspaceDir

## Location
[src/backend/storage/file/reinit.c:161-379](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/reinit.c#L161-L379)

## Overview
ResetUnloggedRelationsInDbspaceDir is the core function that processes unlogged relations within a specific database directory, performing both cleanup and initialization operations on relation files with advanced hash-based tracking for efficiency.

## Definition
```c
static void ResetUnloggedRelationsInDbspaceDir(const char *dbspacedirname, int op)
```

## Detailed Description
This function is the workhorse of the unlogged relation reset system, operating at the database directory level. It performs two distinct operations based on the operation flags:

### Cleanup Operation (UNLOGGED_RELATION_CLEANUP)
Implements a sophisticated two-pass cleanup algorithm:
1. **First Pass**: Scans the directory to identify all relations with init forks, storing their RelFileNumbers in a hash table for O(1) lookup performance
2. **Second Pass**: Removes all non-init fork files that correspond to relations found in the hash table

The hash-based approach ensures O(n) performance rather than O(n²) when dealing with many unlogged relations in the same database.

### Initialization Operation (UNLOGGED_RELATION_INIT)
Restores unlogged relations to their initial state through a three-phase process:
1. **Copy Phase**: Copies init fork files to their corresponding main fork files
2. **Sync Phase**: Performs fsync on all newly created main fork files to ensure durability
3. **Directory Sync**: Syncs the database directory itself to ensure filesystem metadata persistence

## Parameters / Member Variables
- `dbspacedirname`: Path to the database directory within a tablespace (e.g., "base/16384", "pg_tblspc/16385/PG_17_6/16384")
- `op`: Bitwise operation flags specifying which operations to perform
  - `UNLOGGED_RELATION_CLEANUP` (0x0001): Enable cleanup of relation forks  
  - `UNLOGGED_RELATION_INIT` (0x0002): Enable initialization from init forks

## Dependencies
- Functions called/Symbols referenced:
  - `[parse_filename_for_nontemp_relation](../p/parse_filename_for_nontemp_relation.md)`: Parses relation filenames to extract components
  - `[hash_create](../h/hash_create.md)`/`hash_search`/`hash_destroy`: Hash table operations for RelFileNumber tracking
  - `[AllocateDir](../A/AllocateDir.md)`/`ReadDir`/`FreeDir`: Directory traversal operations
  - `[copy_file](../c/copy_file.md)`: File copying operation for init-to-main fork copying
  - `[fsync_fname](../f/fsync_fname.md)`: File synchronization for durability
  - `unlink`: File deletion during cleanup
  - `ereport`/`elog`: Error reporting and debug logging

- Data structures:
  - `unlogged_relation_entry`: Hash table entry containing RelFileNumber as key
  - `[HTAB](../H/HTAB.md)`: Hash table for tracking relations with init forks

- Called from:
  - `[ResetUnloggedRelationsInTablespaceDir](ResetUnloggedRelationsInTablespaceDir.md)`: For each database directory (line 151)

## Notes and Other Information
- This is a static function, internal to the reinit.c module
- Uses efficient hash table to avoid O(n²) performance when many unlogged relations exist
- The cleanup phase is optimized to early-exit if no init forks are found
- File copying includes proper error handling and debug logging
- The sync phase is separated to allow kernel to optimize metadata operations
- Directory fsync ensures filesystem consistency after file operations
- The function handles relation file segments (e.g., relation.1, relation.2) correctly
- Initialization always happens after cleanup to ensure proper ordering
- Located in src/backend/storage/file/reinit.c:161-379

## Simplified Source

```c
static void ResetUnloggedRelationsInDbspaceDir(const char *dbspacedirname, int op)
{
    DIR *dbspace_dir;
    struct dirent *de;
    char rm_path[MAXPGPATH * 2];

    Assert((op & (UNLOGGED_RELATION_CLEANUP | UNLOGGED_RELATION_INIT)) != 0);

    // CLEANUP PHASE: Remove non-init fork files for relations with init forks
    if ((op & UNLOGGED_RELATION_CLEANUP) != 0)
    {
        HTAB *hash;
        HASHCTL ctl;

        // Create hash table to track relations with init forks
        ctl.keysize = sizeof(Oid);
        ctl.entrysize = sizeof(unlogged_relation_entry);
        ctl.hcxt = CurrentMemoryContext;
        hash = hash_create("unlogged relation OIDs", 32, &ctl,
                           HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

        // First pass: identify all relations with init forks
        dbspace_dir = AllocateDir(dbspacedirname);
        while ((de = ReadDir(dbspace_dir, dbspacedirname)) != NULL)
        {
            ForkNumber forkNum;
            unsigned segno;
            unlogged_relation_entry ent;

            if (!parse_filename_for_nontemp_relation(de->d_name,
                                                     &ent.relnumber,
                                                     &forkNum, &segno))
                continue;

            if (forkNum != INIT_FORKNUM)
                continue;

            // Add to hash table
            (void) hash_search(hash, &ent, HASH_ENTER, NULL);
        }
        FreeDir(dbspace_dir);

        // Early exit if no init forks found
        if (hash_get_num_entries(hash) == 0)
        {
            hash_destroy(hash);
            return;
        }

        // Second pass: remove non-init files for relations with init forks
        dbspace_dir = AllocateDir(dbspacedirname);
        while ((de = ReadDir(dbspace_dir, dbspacedirname)) != NULL)
        {
            ForkNumber forkNum;
            unsigned segno;
            unlogged_relation_entry ent;

            if (!parse_filename_for_nontemp_relation(de->d_name,
                                                     &ent.relnumber,
                                                     &forkNum, &segno))
                continue;

            if (forkNum == INIT_FORKNUM)
                continue;

            // Remove file if relation has init fork
            if (hash_search(hash, &ent, HASH_FIND, NULL))
            {
                snprintf(rm_path, sizeof(rm_path), "%s/%s",
                         dbspacedirname, de->d_name);
                if (unlink(rm_path) < 0)
                    ereport(ERROR, (errcode_for_file_access(),
                                   errmsg("could not remove file \"%s\": %m",
                                          rm_path)));
                else
                    elog(DEBUG2, "unlinked file \"%s\"", rm_path);
            }
        }
        FreeDir(dbspace_dir);
        hash_destroy(hash);
    }

    // INITIALIZATION PHASE: Copy init forks to main forks
    if ((op & UNLOGGED_RELATION_INIT) != 0)
    {
        // Copy init fork files to main fork files
        dbspace_dir = AllocateDir(dbspacedirname);
        while ((de = ReadDir(dbspace_dir, dbspacedirname)) != NULL)
        {
            ForkNumber forkNum;
            RelFileNumber relNumber;
            unsigned segno;
            char srcpath[MAXPGPATH * 2];
            char dstpath[MAXPGPATH];

            if (!parse_filename_for_nontemp_relation(de->d_name, &relNumber,
                                                     &forkNum, &segno))
                continue;

            if (forkNum != INIT_FORKNUM)
                continue;

            // Construct source and destination paths
            snprintf(srcpath, sizeof(srcpath), "%s/%s",
                     dbspacedirname, de->d_name);

            if (segno == 0)
                snprintf(dstpath, sizeof(dstpath), "%s/%u",
                         dbspacedirname, relNumber);
            else
                snprintf(dstpath, sizeof(dstpath), "%s/%u.%u",
                         dbspacedirname, relNumber, segno);

            // Copy the file
            elog(DEBUG2, "copying %s to %s", srcpath, dstpath);
            copy_file(srcpath, dstpath);
        }
        FreeDir(dbspace_dir);

        // Fsync all the newly created main fork files
        dbspace_dir = AllocateDir(dbspacedirname);
        while ((de = ReadDir(dbspace_dir, dbspacedirname)) != NULL)
        {
            RelFileNumber relNumber;
            ForkNumber forkNum;
            unsigned segno;
            char mainpath[MAXPGPATH];

            if (!parse_filename_for_nontemp_relation(de->d_name, &relNumber,
                                                     &forkNum, &segno))
                continue;

            if (forkNum != INIT_FORKNUM)
                continue;

            // Construct main fork path and fsync
            if (segno == 0)
                snprintf(mainpath, sizeof(mainpath), "%s/%u",
                         dbspacedirname, relNumber);
            else
                snprintf(mainpath, sizeof(mainpath), "%s/%u.%u",
                         dbspacedirname, relNumber, segno);

            fsync_fname(mainpath, false);
        }
        FreeDir(dbspace_dir);

        // Fsync the database directory
        fsync_fname(dbspacedirname, true);
    }
}
```