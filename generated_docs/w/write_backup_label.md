# write_backup_label

## Location
[src/bin/pg_combinebackup/backup_label.c:127-200](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_combinebackup/backup_label.c#L127-L200)

## Overview
Creates a new backup label file in the output directory based on an input backup label, filtering out incremental backup-specific lines and computing checksums.

## Definition

```c
struct stat sb;
```
## Detailed Description
The  function generates a new backup_label file by copying the contents of an input backup label buffer while excluding incremental backup-specific information. It creates a file named "backup_label" in the specified output directory, omitting lines that start with "INCREMENTAL FROM LSN:" and "INCREMENTAL FROM TLI:".

The function performs several key operations:
1. Creates and opens the output backup_label file with exclusive creation flags
2. Iterates through the input buffer line by line
3. Filters out incremental backup-specific lines 
4. Writes remaining content to the output file
5. Computes checksums during the write process
6. Optionally adds the file to a backup manifest with metadata

This is essential for creating clean backup label files when combining incremental backups, ensuring the final backup label represents the combined backup state without referencing previous incremental stages.

## Parameters / Member Variables
- : Directory path where the new backup_label file will be created
- : StringInfo buffer containing the source backup label content to process
- : Type of checksum algorithm to use for file integrity verification
- : Optional manifest writer for adding the file to backup manifests (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [pg_checksum_init](../p/pg_checksum_init.md)
  - [get_eol_offset](../g/get_eol_offset.md)
  - [line_starts_with](../l/line_starts_with.md)
  - open
  - write
  - [pg_checksum_update](../p/pg_checksum_update.md)
  - close
  - [pg_checksum_final](../p/pg_checksum_final.md)
  - [add_file_to_manifest](../a/add_file_to_manifest.md)
  - [stat](../s/stat.md)
- Called from (representative examples):
  - [main](../m/main.md) (in pg_combinebackup.c)

## Notes and Other Information
- Uses O_EXCL flag to ensure the backup_label file doesn't already exist, preventing accidental overwrites
- Maintains checksums throughout the write process for data integrity verification
- File permissions are set using pg_file_create_mode for consistent PostgreSQL file permissions
- If manifest writer is provided, the function automatically adds the new file to the backup manifest with size, modification time, and checksum information
- Part of the pg_combinebackup utility infrastructure for merging incremental backups into full backups

## Simplified Source

```c
void
write_backup_label(char *output_directory, StringInfo buf,
                   pg_checksum_type checksum_type, manifest_writer *mwriter)
{
    char output_filename[MAXPGPATH];
    int output_fd;
    pg_checksum_context checksum_ctx;
    uint8 checksum_payload[PG_CHECKSUM_MAX_LENGTH];
    int checksum_length;

    // Initialize checksum calculation
    pg_checksum_init(&checksum_ctx, checksum_type);

    // Create output file path
    snprintf(output_filename, MAXPGPATH, "%s/backup_label", output_directory);

    // Open output file for writing
    if ((output_fd = open(output_filename,
                          O_WRONLY | O_CREAT | O_EXCL | PG_BINARY,
                          pg_file_create_mode)) < 0)
        pg_fatal("could not open file \"%s\": %m", output_filename);

    // Process input buffer line by line
    while (buf->cursor < buf->len)
    {
        char *line_start = &buf->data[buf->cursor];
        int eol_offset = get_eol_offset(buf);
        char *line_end = &buf->data[eol_offset];

        // Skip incremental backup lines
        if (!line_starts_with(line_start, line_end, "INCREMENTAL FROM LSN: ", NULL) &&
            !line_starts_with(line_start, line_end, "INCREMENTAL FROM TLI: ", NULL))
        {
            // Write line to output file
            ssize_t bytes_written = write(output_fd, line_start, line_end - line_start);
            if (bytes_written != line_end - line_start)
                pg_fatal("write error to file \"%s\"", output_filename);

            // Update running checksum
            if (pg_checksum_update(&checksum_ctx, (uint8 *) line_start,
                                 line_end - line_start) < 0)
                pg_fatal("could not update checksum of file \"%s\"", output_filename);
        }

        buf->cursor = eol_offset;
    }

    // Close output file
    if (close(output_fd) != 0)
        pg_fatal("could not close file \"%s\": %m", output_filename);

    // Finalize checksum
    checksum_length = pg_checksum_final(&checksum_ctx, checksum_payload);

    // Add to manifest if writer provided
    if (mwriter != NULL)
    {
        struct stat sb;
        if (stat(output_filename, &sb) < 0)
            pg_fatal("could not stat file \"%s\": %m", output_filename);

        add_file_to_manifest(mwriter, "backup_label", sb.st_size,
                           sb.st_mtime, checksum_type,
                           checksum_length, checksum_payload);
    }
}
```