# scan_for_existing_tablespaces

## Location
[src/bin/pg_combinebackup/pg_combinebackup.c:1245-1375](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_combinebackup/pg_combinebackup.c#L1245-L1375)

## Overview
Scans the pg_tblspc directory of the final input backup to create a canonical list of tablespaces that are part of the backup, handling both in-place and relocated tablespaces.

## Definition
```c
static cb_tablespace *scan_for_existing_tablespaces(char *pathname, cb_options *opt)
```

## Detailed Description
This function examines the pg_tblspc directory within a backup directory to identify all tablespaces. It processes both symbolic links (representing external tablespaces) and directories (representing in-place tablespaces). For symbolic links, it reads the link target, validates it, and matches it with provided tablespace mappings. For directories, it treats them as in-place tablespaces. The function creates a linked list of cb_tablespace structures containing tablespace information including OIDs, old/new directory paths, and in-place status. It performs extensive validation including OID parsing, path canonicalization, and ensures no duplicate tablespace destinations.

## Parameters / Member Variables
- `pathname`: Path to the toplevel backup directory for the final backup in the backup chain
- `opt`: cb_options structure containing program options including tablespace mappings

## Dependencies
- Functions called/Symbols referenced:
  - [DIR](../D/DIR.md), dirent (directory handling types)
  - [cb_tablespace](../c/cb_tablespace.md), cb_options, cb_tablespace_mapping (structure types)
  - pg_log_debug (logging function)
  - [opendir](../o/opendir.md), readdir, closedir (directory operations)
  - [parse_oid](../p/parse_oid.md) (OID parsing utility)
  - [get_dirent_type](../g/get_dirent_type.md) (file type detection)
  - [pg_malloc0](../p/pg_malloc0.md) (zero-initialized memory allocation)
  - readlink (symbolic link reading)
  - is_absolute_path (path validation)
  - [canonicalize_path](../c/canonicalize_path.md) (path canonicalization)
  - [strlcpy](strlcpy.md) (safe string copying)
- Called from (representative examples):
  - [main](../m/main.md) (in src/bin/pg_combinebackup/pg_combinebackup.c:311)

## Notes and Other Information
- This is a static function used specifically within pg_combinebackup utility
- Returns a linked list of cb_tablespace structures representing discovered tablespaces
- Handles both external tablespaces (symbolic links) and in-place tablespaces (directories)
- Requires tablespace mappings for all external tablespaces or will fatal error
- Performs validation to prevent tablespace conflicts and invalid configurations
- Uses errno handling for proper directory iteration error detection
- File location: src/bin/pg_combinebackup/pg_combinebackup.c:1245-1375

## Simplified Source

```c
static cb_tablespace *scan_for_existing_tablespaces(char *pathname, cb_options *opt) {
    char pg_tblspc[MAXPGPATH];
    DIR *dir;
    struct dirent *de;
    cb_tablespace *tslist = NULL;

    // Open pg_tblspc directory
    snprintf(pg_tblspc, MAXPGPATH, "%s/pg_tblspc", pathname);
    if ((dir = opendir(pg_tblspc)) == NULL)
        pg_fatal("could not open directory \"%s\": %m", pg_tblspc);

    // Scan for tablespace entries
    while ((de = readdir(dir)) != NULL) {
        Oid oid;
        char tblspcdir[MAXPGPATH];
        PGFileType type;

        // Skip . and .. entries
        if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
            continue;

        // Only process valid OID names
        if (!parse_oid(de->d_name, &oid))
            continue;

        snprintf(tblspcdir, MAXPGPATH, "%s/%s", pg_tblspc, de->d_name);
        type = get_dirent_type(tblspcdir, de, false, PG_LOG_ERROR);

        // Only process symlinks and directories
        if (type != PGFILETYPE_LNK && type != PGFILETYPE_DIR)
            continue;

        // Create tablespace entry
        cb_tablespace *ts = pg_malloc0(sizeof(cb_tablespace));
        ts->oid = oid;

        if (type == PGFILETYPE_LNK) {
            // Handle external tablespace (symlink)
            char link_target[MAXPGPATH];
            int link_length = readlink(tblspcdir, link_target, sizeof(link_target));

            if (link_length < 0 || link_length >= sizeof(link_target))
                pg_fatal("could not read symbolic link \"%s\"", tblspcdir);

            link_target[link_length] = '\0';
            canonicalize_path(link_target);

            // Find matching tablespace mapping
            cb_tablespace_mapping *tsmap;
            for (tsmap = opt->tsmappings; tsmap != NULL; tsmap = tsmap->next) {
                if (strcmp(tsmap->old_dir, link_target) == 0) {
                    strlcpy(ts->old_dir, tsmap->old_dir, MAXPGPATH);
                    strlcpy(ts->new_dir, tsmap->new_dir, MAXPGPATH);
                    ts->in_place = false;
                    break;
                }
            }

            if (tsmap == NULL)
                pg_fatal("tablespace at \"%s\" has no tablespace mapping", link_target);
        } else {
            // Handle in-place tablespace (directory)
            snprintf(ts->old_dir, MAXPGPATH, "%s/%s", pg_tblspc, de->d_name);
            snprintf(ts->new_dir, MAXPGPATH, "%s/pg_tblspc/%s", opt->output, de->d_name);
            ts->in_place = true;
        }

        // Check for duplicate destinations
        for (cb_tablespace *otherts = tslist; otherts != NULL; otherts = otherts->next) {
            if (strcmp(ts->new_dir, otherts->new_dir) == 0)
                pg_fatal("tablespaces with OIDs %u and %u both point at directory \"%s\"",
                         otherts->oid, oid, ts->new_dir);
        }

        // Add to list
        ts->next = tslist;
        tslist = ts;
    }

    closedir(dir);
    return tslist;
}
```