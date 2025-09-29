# sendDir

## Location
[src/backend/backup/basebackup.c:1187-1571](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup.c#L1187-L1571)

## Overview
sendDir recursively includes all files from a given directory in the output tar stream during PostgreSQL base backup operations, with comprehensive filtering and special handling for various file types.

## Definition
```c
static int64 sendDir(bbsink *sink, const char *path, int basepathlen, bool sizeonly,
                    List *tablespaces, bool sendtblspclinks, backup_manifest_info *manifest,
                    Oid spcoid, IncrementalBackupInfo *ib)
```

## Detailed Description
This function is the core directory traversal component of PostgreSQL's base backup system. It recursively processes directory contents, applying sophisticated filtering logic to determine which files to include or exclude. The function handles relation files, temporary files, unlogged tables, tablespace symlinks, and various PostgreSQL-specific directories with special processing rules. It supports both full and incremental backup modes, can operate in size-only calculation mode, and maintains backup manifest information throughout the process.

Key processing logic includes:
- Detection of database directories containing relations
- Exclusion of temporary files, unlogged relations (except init forks), and system files
- Special handling of pg_wal, pg_tblspc, and other PostgreSQL directories
- Support for incremental backups with block-level granularity
- Recursive directory traversal with tablespace awareness

## Parameters / Member Variables
- `sink`: bbsink object representing the backup destination stream
- `path`: File system path to the directory being processed
- `basepathlen`: Length of the base path for tar header name calculation
- `sizeonly`: Boolean flag - if true, only calculates total size without sending data
- `tablespaces`: List of tablespace information to avoid duplicate backups
- `sendtblspclinks`: Boolean flag indicating whether to include tablespace symlink information
- `manifest`: Pointer to backup manifest information structure for tracking backup contents
- `spcoid`: Object identifier (OID) of the current tablespace
- `ib`: Pointer to incremental backup information structure (NULL for full backups)

## Dependencies
- Functions called/Symbols referenced:
  - [AllocateDir](../A/AllocateDir.md), ReadDir, FreeDir
  - lstat, readlink
  - [parse_filename_for_nontemp_relation](../p/parse_filename_for_nontemp_relation.md)
  - [looks_like_temp_rel_name](../l/looks_like_temp_rel_name.md)
  - [sendFile](sendFile.md)
  - [_tarWriteHeader](../t/_tarWriteHeader.md)
  - [GetFileBackupMethod](../G/GetFileBackupMethod.md)
  - [RecoveryInProgress](../R/RecoveryInProgress.md)
  - [convert_link_to_directory](../c/convert_link_to_directory.md)
- Called from (representative examples):
  - [perform_base_backup](../p/perform_base_backup.md)
  - [sendTablespace](sendTablespace.md)
  - [sendDir](sendDir.md) (recursive calls)

## Notes and Other Information
- Recursively calls itself for subdirectories, making it the primary directory traversal mechanism
- Implements complex filtering logic to exclude temporary files, unlogged relations, and system-specific files
- Handles incremental backups by determining which file blocks need to be backed up
- Special processing for pg_wal directory (included as empty) and pg_tblspc (symlink handling)
- Supports interruption checking and recovery state validation during long-running operations
- Uses a large BlockNumber array (RELSEG_SIZE) allocated on heap for incremental backup block tracking
- Located in src/backend/backup/basebackup.c:1187-1571

## Simplified Source

```c
// Simplified version of sendDir
static int64
sendDir(bbsink *sink, const char *path, int basepathlen, bool sizeonly,
        List *tablespaces, bool sendtblspclinks, backup_manifest_info *manifest,
        Oid spcoid, IncrementalBackupInfo *ib)
{
    DIR *dir;
    struct dirent *de;
    char pathbuf[MAXPGPATH * 2];
    struct stat statbuf;
    int64 size = 0;
    const char *lastDir;
    bool isRelationDir = false;
    bool isGlobalDir = false;
    Oid dboid = InvalidOid;
    BlockNumber *relative_block_numbers = NULL;

    // Allocate block numbers array for incremental backups
    if (ib != NULL)
        relative_block_numbers = palloc(sizeof(BlockNumber) * RELSEG_SIZE);

    // Determine if this is a database directory that can contain relations
    lastDir = last_dir_separator(path);

    // Check if path looks like a database directory (all digits)
    if (lastDir != NULL && strspn(lastDir + 1, "0123456789") == strlen(lastDir + 1)) {
        int parentPathLen = lastDir - path;

        // Mark as relation directory if parent is base or tablespace version directory
        if (strncmp(path, "./base", parentPathLen) == 0 ||
            (parentPathLen >= (sizeof(TABLESPACE_VERSION_DIRECTORY) - 1) &&
             strncmp(lastDir - (sizeof(TABLESPACE_VERSION_DIRECTORY) - 1),
                     TABLESPACE_VERSION_DIRECTORY,
                     sizeof(TABLESPACE_VERSION_DIRECTORY) - 1) == 0)) {
            isRelationDir = true;
            dboid = atooid(lastDir + 1);
        }
    }
    else if (strcmp(path, "./global") == 0) {
        isRelationDir = true;
        isGlobalDir = true;
    }

    // Open directory and process each entry
    dir = AllocateDir(path);
    while ((de = ReadDir(dir, path)) != NULL) {
        bool excludeFound = false;
        RelFileNumber relfilenumber = InvalidRelFileNumber;
        ForkNumber relForkNum = InvalidForkNumber;
        unsigned segno = 0;
        bool isRelationFile = false;

        // Skip special entries (., ..)
        if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
            continue;

        // Skip temporary files and system files
        if (strncmp(de->d_name, PG_TEMP_FILE_PREFIX, strlen(PG_TEMP_FILE_PREFIX)) == 0 ||
            strcmp(de->d_name, ".DS_Store") == 0)
            continue;

        // Check for interrupts and recovery state changes
        CHECK_FOR_INTERRUPTS();
        if (RecoveryInProgress() != backup_started_in_recovery)
            ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                           errmsg("the standby was promoted during online backup")));

        // Check exclude list
        for (int excludeIdx = 0; excludeFiles[excludeIdx].name != NULL; excludeIdx++) {
            int cmplen = strlen(excludeFiles[excludeIdx].name);
            if (!excludeFiles[excludeIdx].match_prefix)
                cmplen++;
            if (strncmp(de->d_name, excludeFiles[excludeIdx].name, cmplen) == 0) {
                excludeFound = true;
                break;
            }
        }
        if (excludeFound)
            continue;

        // Parse relation file names if in a relation directory
        if (isRelationDir)
            isRelationFile = parse_filename_for_nontemp_relation(de->d_name,
                                                                &relfilenumber,
                                                                &relForkNum, &segno);

        // Skip unlogged tables (except init fork)
        if (isRelationFile && relForkNum != INIT_FORKNUM) {
            char initForkFile[MAXPGPATH];
            snprintf(initForkFile, sizeof(initForkFile), "%s/%u_init", path, relfilenumber);
            if (lstat(initForkFile, &statbuf) == 0)
                continue; // Skip unlogged relation files
        }

        // Skip temporary relations
        if (OidIsValid(dboid) && looks_like_temp_rel_name(de->d_name))
            continue;

        snprintf(pathbuf, sizeof(pathbuf), "%s/%s", path, de->d_name);

        // Skip pg_control (backed up separately)
        if (strcmp(pathbuf, "./global/pg_control") == 0)
            continue;

        // Get file stats
        if (lstat(pathbuf, &statbuf) != 0) {
            if (errno != ENOENT)
                ereport(ERROR, (errcode_for_file_access(),
                               errmsg("could not stat file or directory \"%s\"", pathbuf)));
            continue; // File disappeared, not an error
        }

        // Handle directories with excluded contents
        for (int excludeIdx = 0; excludeDirContents[excludeIdx] != NULL; excludeIdx++) {
            if (strcmp(de->d_name, excludeDirContents[excludeIdx]) == 0) {
                convert_link_to_directory(pathbuf, &statbuf);
                size += _tarWriteHeader(sink, pathbuf + basepathlen + 1, NULL,
                                      &statbuf, sizeonly);
                excludeFound = true;
                break;
            }
        }
        if (excludeFound)
            continue;

        // Special handling for pg_wal directory
        if (strcmp(pathbuf, "./pg_wal") == 0) {
            convert_link_to_directory(pathbuf, &statbuf);
            size += _tarWriteHeader(sink, pathbuf + basepathlen + 1, NULL, &statbuf, sizeonly);
            // Add archive_status and summaries subdirectories
            size += _tarWriteHeader(sink, "./pg_wal/archive_status", NULL, &statbuf, sizeonly);
            size += _tarWriteHeader(sink, "./pg_wal/summaries", NULL, &statbuf, sizeonly);
            continue; // Don't recurse into pg_wal
        }

        // Handle symbolic links (only in pg_tblspc)
        if (strcmp(path, "./pg_tblspc") == 0 && S_ISLNK(statbuf.st_mode)) {
            char linkpath[MAXPGPATH];
            int rllen = readlink(pathbuf, linkpath, sizeof(linkpath));
            if (rllen < 0 || rllen >= sizeof(linkpath))
                ereport(ERROR, (errcode_for_file_access(),
                               errmsg("could not read symbolic link \"%s\"", pathbuf)));
            linkpath[rllen] = '\0';
            size += _tarWriteHeader(sink, pathbuf + basepathlen + 1, linkpath, &statbuf, sizeonly);
        }
        // Handle directories
        else if (S_ISDIR(statbuf.st_mode)) {
            bool skip_this_dir = false;

            // Add directory header
            size += _tarWriteHeader(sink, pathbuf + basepathlen + 1, NULL, &statbuf, sizeonly);

            // Check if this directory is a tablespace to skip
            foreach(lc, tablespaces) {
                tablespaceinfo *ti = (tablespaceinfo *) lfirst(lc);
                if (ti->rpath && strcmp(ti->rpath, pathbuf + 2) == 0) {
                    skip_this_dir = true;
                    break;
                }
            }

            // Skip pg_tblspc contents if not sending tablespace links
            if (strcmp(pathbuf, "./pg_tblspc") == 0 && !sendtblspclinks)
                skip_this_dir = true;

            // Recursively process subdirectory
            if (!skip_this_dir)
                size += sendDir(sink, pathbuf, basepathlen, sizeonly, tablespaces,
                              sendtblspclinks, manifest, spcoid, ib);
        }
        // Handle regular files
        else if (S_ISREG(statbuf.st_mode)) {
            FileBackupMethod method = BACK_UP_FILE_FULLY;
            unsigned num_blocks_required = 0;
            unsigned truncation_block_length = 0;
            char *tarfilename = pathbuf + basepathlen + 1;

            // Determine backup method for incremental backups
            if (ib != NULL && isRelationFile) {
                Oid relspcoid = isGlobalDir ? GLOBALTABLESPACE_OID :
                               (OidIsValid(spcoid) ? spcoid : DEFAULTTABLESPACE_OID);
                char *lookup_path = OidIsValid(spcoid) ?
                                   psprintf("pg_tblspc/%u/%s", spcoid, tarfilename) :
                                   pstrdup(tarfilename);

                method = GetFileBackupMethod(ib, lookup_path, dboid, relspcoid,
                                           relfilenumber, relForkNum, segno, statbuf.st_size,
                                           &num_blocks_required, relative_block_numbers,
                                           &truncation_block_length);

                // Adjust for incremental backup
                if (method == BACK_UP_FILE_INCREMENTALLY) {
                    statbuf.st_size = GetIncrementalFileSize(num_blocks_required);
                    // Modify tarfilename for incremental files
                    char tarfilenamebuf[MAXPGPATH * 2];
                    snprintf(tarfilenamebuf, sizeof(tarfilenamebuf), "%s/INCREMENTAL.%s",
                            path + basepathlen + 1, de->d_name);
                    tarfilename = tarfilenamebuf;
                }
                pfree(lookup_path);
            }

            // Send the file (or just calculate size)
            bool sent = false;
            if (!sizeonly)
                sent = sendFile(sink, pathbuf, tarfilename, &statbuf, true, dboid, spcoid,
                              relfilenumber, segno, manifest, num_blocks_required,
                              method == BACK_UP_FILE_INCREMENTALLY ? relative_block_numbers : NULL,
                              truncation_block_length);

            if (sent || sizeonly) {
                size += statbuf.st_size;
                size += tarPaddingBytesRequired(statbuf.st_size);
                size += TAR_BLOCK_SIZE; // Header size
            }
        }
        else {
            ereport(WARNING, (errmsg("skipping special file \"%s\"", pathbuf)));
        }
    }

    // Cleanup
    if (relative_block_numbers != NULL)
        pfree(relative_block_numbers);
    FreeDir(dir);

    return size;
}
```

Key simplifications made:
- Consolidated error handling into essential checks only
- Simplified complex conditional logic into clearer if-else chains
- Removed detailed error messages and kept core functionality
- Abstracted platform-specific code details
- Added descriptive comments for major logic sections
- Simplified variable declarations and reduced nesting depth
- Maintained all core algorithm logic and file processing rules