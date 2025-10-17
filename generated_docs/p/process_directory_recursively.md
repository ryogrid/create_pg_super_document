# process_directory_recursively

## Location
[src/bin/pg_combinebackup/pg_combinebackup.c:823-1153](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_combinebackup/pg_combinebackup.c#L823-L1153)

## Overview
Recursively processes directory structures to reconstruct full files from incremental backup files and copies regular files, handling the complete directory tree traversal for backup combination operations.

## Definition

```c
static void process_directory_recursively(Oid tsoid,
                                          char *input_directory,
                                          char *output_directory,
                                          char *relative_path,
                                          int n_prior_backups,
                                          char **prior_backup_dirs,
                                          manifest_data **manifests,
                                          manifest_writer *mwriter,
                                          cb_options *opt)
```
## Detailed Description
This is the core function of pg_combinebackup that handles the recursive processing of PostgreSQL backup directory structures. It performs several critical operations:

1. **Directory Classification**: Identifies special PostgreSQL directories (pg_tblspc, pg_wal, base, global) that require different handling
2. **File Type Processing**: Handles different file types including incremental files (with INCREMENTAL. prefix), regular files, directories, and symbolic links
3. **Incremental File Reconstruction**: Delegates incremental files to reconstruction logic that combines them with prior backup data
4. **Regular File Copying**: Copies non-incremental files directly to the output directory
5. **Manifest Integration**: Reuses checksums from backup manifests when available and generates new manifest entries
6. **Tablespace Handling**: Properly processes both regular tablespaces and in-place tablespaces

The function intelligently skips certain files (backup_label, backup_manifest) and directories (tablespace OID directories in pg_tblspc) that are handled elsewhere in the backup combination process.

## Parameters / Member Variables
- : OID of the tablespace being processed (InvalidOid for main data directory)
- : Source directory path for the current backup
- : Destination directory path for combined output
- : Current subdirectory being processed relative to input/output directories
- : Number of previous backup directories available for reconstruction
- : Array of paths to previous backup directories
- : Array of backup manifest data structures
- : Manifest writer for generating output manifest entries
- : Options structure containing operation settings (dry_run, copy_method, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - : Determines file type from directory entry
  - : Parses OID from directory names
  - : Reconstructs files from incremental data
  - : Copies regular files between directories
  - : Adds entries to backup manifest
  - , : Checksum calculation functions
  - : Looks up file entries in backup manifests
  - , , : Directory traversal functions
  - , : Filesystem operations
- Called from (representative examples):
  -  (in src/bin/pg_combinebackup/pg_combinebackup.c:358)
  -  (in src/bin/pg_combinebackup/pg_combinebackup.c:405)
  -  (recursive self-call at line 983)

## Notes and Other Information
- The function is highly specialized for PostgreSQL backup structures and understands the layout of data directories, tablespaces, and WAL directories
- Incremental files are identified by the INCREMENTAL. prefix and require special reconstruction logic that combines data from multiple backup layers
- WAL directory files are processed without checksums since they're not included in backup manifests
- The function handles both dry-run mode (logging what would be done) and actual execution
- Checksum reuse optimization: when available, checksums from backup manifests are reused instead of recalculating them
- Special handling for pg_tblspc directory prevents recursion into tablespace directories that are processed separately
- The function maintains proper error handling with fatal errors for critical failures and warnings for non-critical issues
- Self-recursive design allows it to handle arbitrarily deep directory structures while maintaining context about the current processing state

## Simplified Source

```c
static void process_directory_recursively(Oid tsoid,
                                          char *input_directory,
                                          char *output_directory,
                                          char *relative_path,
                                          int n_prior_backups,
                                          char **prior_backup_dirs,
                                          manifest_data **manifests,
                                          manifest_writer *mwriter,
                                          cb_options *opt) {
    char ifulldir[MAXPGPATH], ofulldir[MAXPGPATH];
    DIR *dir;
    struct dirent *de;

    // Classify directory type for special handling
    bool is_pg_tblspc = (relative_path && strcmp(relative_path, "pg_tblspc") == 0);
    bool is_pg_wal = (relative_path && strncmp(relative_path, "pg_wal", 6) == 0);
    bool is_incremental_dir = (OidIsValid(tsoid) ||
                               (relative_path && (strncmp(relative_path, "base/", 5) == 0 ||
                                                  strcmp(relative_path, "global") == 0 ||
                                                  strncmp(relative_path, "pg_tblspc/", 10) == 0)));

    // Build full directory paths
    if (relative_path == NULL) {
        strlcpy(ifulldir, input_directory, MAXPGPATH);
        strlcpy(ofulldir, output_directory, MAXPGPATH);
    } else {
        snprintf(ifulldir, MAXPGPATH, "%s/%s", input_directory, relative_path);
        snprintf(ofulldir, MAXPGPATH, "%s/%s", output_directory, relative_path);

        // Create output subdirectory
        if (!opt->dry_run && mkdir(ofulldir, pg_dir_create_mode) == -1)
            pg_fatal("could not create directory \"%s\": %m", ofulldir);
    }

    // Scan directory entries
    if ((dir = opendir(ifulldir)) == NULL)
        pg_fatal("could not open directory \"%s\": %m", ifulldir);

    while ((de = readdir(dir)) != NULL) {
        // Skip . and .. entries
        if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
            continue;

        char ifullpath[MAXPGPATH], ofullpath[MAXPGPATH];
        snprintf(ifullpath, MAXPGPATH, "%s/%s", ifulldir, de->d_name);

        PGFileType type = get_dirent_type(ifullpath, de, false, PG_LOG_ERROR);

        // Skip tablespace directories in pg_tblspc
        if (is_pg_tblspc && parse_oid(de->d_name, &oid) &&
            (type == PGFILETYPE_LNK || type == PGFILETYPE_DIR))
            continue;

        // Recurse into subdirectories
        if (type == PGFILETYPE_DIR) {
            char new_relative_path[MAXPGPATH];
            if (relative_path == NULL)
                strlcpy(new_relative_path, de->d_name, MAXPGPATH);
            else
                snprintf(new_relative_path, MAXPGPATH, "%s/%s", relative_path, de->d_name);

            process_directory_recursively(tsoid, input_directory, output_directory,
                                          new_relative_path, n_prior_backups,
                                          prior_backup_dirs, manifests, mwriter, opt);
            continue;
        }

        // Process only regular files
        if (type != PGFILETYPE_REG)
            continue;

        // Skip special files handled elsewhere
        if (relative_path == NULL &&
            (strcmp(de->d_name, "backup_label") == 0 ||
             strcmp(de->d_name, "backup_manifest") == 0))
            continue;

        // Handle incremental files
        if (is_incremental_dir &&
            strncmp(de->d_name, INCREMENTAL_PREFIX, INCREMENTAL_PREFIX_LENGTH) == 0) {
            // Remove INCREMENTAL. prefix for output path
            snprintf(ofullpath, MAXPGPATH, "%s/%s", ofulldir,
                     de->d_name + INCREMENTAL_PREFIX_LENGTH);

            // Reconstruct file from incremental data
            reconstruct_from_incremental_file(ifullpath, ofullpath,
                                             manifest_prefix,
                                             de->d_name + INCREMENTAL_PREFIX_LENGTH,
                                             n_prior_backups, prior_backup_dirs,
                                             manifests, manifest_path,
                                             checksum_type, &checksum_length,
                                             &checksum_payload, opt->copy_method,
                                             opt->debug, opt->dry_run);
        } else {
            // Copy regular file directly
            snprintf(ofullpath, MAXPGPATH, "%s/%s", ofulldir, de->d_name);
            copy_file(ifullpath, ofullpath, &checksum_ctx, opt->copy_method, opt->dry_run);
        }

        // Add to manifest if needed
        if (mwriter != NULL) {
            struct stat sb;
            if (stat(ofullpath, &sb) >= 0) {
                add_file_to_manifest(mwriter, manifest_path, sb.st_size, sb.st_mtime,
                                     checksum_type, checksum_length, checksum_payload);
            }
        }
    }

    closedir(dir);
}
```