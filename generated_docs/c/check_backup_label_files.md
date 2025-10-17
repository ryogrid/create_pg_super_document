# check_backup_label_files

## Location
[src/bin/pg_combinebackup/pg_combinebackup.c:501-593](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_combinebackup/pg_combinebackup.c#L501-L593)

## Overview
Validates that backup_label files form a coherent backup chain and returns the backup_label contents from the latest backup in the chain.

## Definition
```c
static StringInfo check_backup_label_files(int n_backups, char **backup_dirs)
```

## Detailed Description
The check_backup_label_files function verifies the consistency and continuity of a backup chain by examining backup_label files in multiple backup directories. It processes the backup_label files in reverse chronological order (latest to first) and performs comprehensive validation to ensure the backups form a valid incremental backup chain.

Key functionality includes:
1. **File reading**: Reads and parses each backup_label file from the provided directories
2. **Chain validation**: Ensures the backup chain is consistent by verifying timeline IDs and LSN sequences
3. **Backup type validation**: Confirms that only the first backup is a full backup and subsequent ones are incremental
4. **Continuity checking**: Verifies that each backup starts where the previous one ended
5. **Memory management**: Efficiently manages StringInfo buffers to avoid memory waste

The function enforces strict rules about backup chain structure: the first backup must be a full backup (previous_tli == 0), and all subsequent backups must be incremental with proper timeline and LSN continuity.

## Parameters / Member Variables
- `n_backups`: Number of backup directories to process
- `backup_dirs`: Array of backup directory paths to examine

## Dependencies
- Functions called/Symbols referenced:
  - [makeStringInfo](../m/makeStringInfo.md) (create expandable string buffer)
  - pg_log_debug (debug logging)
  - open (file opening)
  - [slurp_file](../s/slurp_file.md) (read entire file into buffer)
  - close (file closing)
  - [parse_backup_label](../p/parse_backup_label.md) (parse backup_label file contents)
  - [resetStringInfo](../r/resetStringInfo.md) (reset string buffer)
  - [destroyStringInfo](../d/destroyStringInfo.md) (free string buffer memory)
- Called from (representative examples):
  - [main](../m/main.md) (backup processing workflow)

## Notes and Other Information
- Located in src/bin/pg_combinebackup/pg_combinebackup.c:501-593
- Returns the backup_label contents from the most recent backup for further processing
- Imposes a reasonable file size limit (10000 + MAXPGPATH bytes) for backup_label files
- Processes backup directories in reverse order (last to first) for efficient validation
- Includes detailed error messages with specific timeline and LSN information for debugging
- Contains a TODO note about potentially allowing start_lsn > check_lsn under certain conditions
- Efficiently manages memory by reusing StringInfo buffers during processing

## Simplified Source

```c
static StringInfo check_backup_label_files(int n_backups, char **backup_dirs) {
    StringInfo buf = makeStringInfo();
    StringInfo lastbuf = buf;
    int i;
    TimeLineID check_tli = 0;
    XLogRecPtr check_lsn = InvalidXLogRecPtr;

    // Process backup_label files from latest to first
    for (i = n_backups - 1; i >= 0; --i) {
        char pathbuf[MAXPGPATH];
        int fd;
        TimeLineID start_tli, previous_tli;
        XLogRecPtr start_lsn, previous_lsn;

        // Read backup_label file
        snprintf(pathbuf, MAXPGPATH, "%s/backup_label", backup_dirs[i]);
        pg_log_debug("reading \"%s\"", pathbuf);
        if ((fd = open(pathbuf, O_RDONLY, 0)) < 0)
            pg_fatal("could not open file \"%s\": %m", pathbuf);

        slurp_file(fd, pathbuf, buf, 10000 + MAXPGPATH);
        if (close(fd) != 0)
            pg_fatal("could not close file \"%s\": %m", pathbuf);

        // Parse backup_label contents
        parse_backup_label(pathbuf, buf, &start_tli, &start_lsn,
                          &previous_tli, &previous_lsn);

        // Validate backup chain consistency
        if (i > 0 && previous_tli == 0)
            pg_fatal("backup at \"%s\" is a full backup, but only the first backup should be a full backup",
                     backup_dirs[i]);
        if (i == 0 && previous_tli != 0)
            pg_fatal("backup at \"%s\" is an incremental backup, but the first backup should be a full backup",
                     backup_dirs[i]);
        if (i < n_backups - 1 && start_tli != check_tli)
            pg_fatal("backup at \"%s\" starts on timeline %u, but expected %u",
                     backup_dirs[i], start_tli, check_tli);
        if (i < n_backups - 1 && start_lsn != check_lsn)
            pg_fatal("backup at \"%s\" starts at LSN %X/%X, but expected %X/%X",
                     backup_dirs[i], LSN_FORMAT_ARGS(start_lsn), LSN_FORMAT_ARGS(check_lsn));

        check_tli = previous_tli;
        check_lsn = previous_lsn;

        // Manage buffers - keep last one, reset others
        if (lastbuf == buf)
            buf = makeStringInfo();
        else
            resetStringInfo(buf);
    }

    // Clean up unused buffer
    if (lastbuf != buf)
        destroyStringInfo(buf);

    return lastbuf;
}
```