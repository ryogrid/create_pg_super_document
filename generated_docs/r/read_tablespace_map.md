# read_tablespace_map

## Location
[src/backend/access/transam/xlogrecovery.c:1354-1457](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogrecovery.c#L1354-L1457)

## Overview
Reads and parses the tablespace_map file during backup recovery to extract tablespace OID-to-path mappings needed for creating proper symlinks in the restored database.

## Definition

```c
static bool
read_tablespace_map(List **tablespaces)
```
## Detailed Description
This function checks for the presence of a tablespace_map file during recovery from a backup dump. When found, it parses the file to extract tablespace information including OID and path mappings. The tablespace_map file is created during backup operations and contains the necessary information to recreate tablespace symlinks in the correct locations during recovery.

The function performs line-by-line parsing of the tablespace_map file format, which contains entries with tablespace OIDs followed by their corresponding filesystem paths. Each line is processed to extract the OID (converted from string to unsigned long) and the associated path string. The function handles backslash escaping within paths and validates the format of each entry.

The parsed tablespace information is returned as a list of tablespaceinfo structs, each containing the OID and path for a single tablespace. This information is later used by the recovery process to create appropriate symlinks.

## Parameters / Member Variables
- `**tablespaces`: Output parameter - pointer to a List that will be populated with tablespaceinfo structs for each tablespace found in the map file
## Dependencies
- Functions called/Symbols referenced:
  - [AllocateFile](../A/AllocateFile.md) (opens tablespace_map file for reading)
  - [FreeFile](../F/FreeFile.md) (closes the tablespace_map file)
  - [palloc0](../p/palloc0.md) (allocates zeroed memory for tablespaceinfo structs)
  - [pstrdup](../p/pstrdup.md) (duplicates path strings)
  - [lappend](../l/lappend.md) (appends tablespaceinfo to the list)
  - TABLESPACE_MAP (tablespace_map filename constant)
  - [tablespaceinfo](../t/tablespaceinfo.md) (struct type for storing tablespace OID and path)
- Called from:
  - [InitWalRecovery](../I/InitWalRecovery.md) (during WAL recovery initialization)

## Notes and Other Information
- Returns false if tablespace_map file doesn't exist (normal case for non-backup recovery)
- Returns true if tablespace_map found and parsed successfully  
- File format expects OID followed by exactly one space followed by path
- Handles backslash escaping within file paths for proper de-escaping
- Validates OID conversion and issues FATAL error for malformed entries
- Each line must be properly terminated or parsing fails with FATAL error
- The parsing is intentionally crude but sufficient for the fixed format produced by backup tools
- Memory for tablespaceinfo structs is allocated in the current memory context

## Simplified Source

```c
// Simplified version of read_tablespace_map
static bool
read_tablespace_map(List **tablespaces)
{
    tablespaceinfo *tablespace_entry;
    FILE *map_file;
    char line_buffer[MAXPGPATH];
    int ch, buffer_pos, space_pos;
    bool was_backslash = false;

    // Step 1: Try to open the tablespace_map file
    map_file = AllocateFile(TABLESPACE_MAP, "r");
    if (!map_file) {
        if (errno != ENOENT)
            ereport(FATAL, (errmsg("could not read file \"%s\"", TABLESPACE_MAP)));
        return false;  // File doesn't exist - normal case
    }

    // Step 2: Parse file character by character
    buffer_pos = 0;
    while ((ch = fgetc(map_file)) != EOF) {

        // Handle line endings - process complete line
        if (!was_backslash && (ch == '\n' || ch == '\r')) {
            if (buffer_pos == 0)
                continue;  // Skip empty lines

            // Step 3: Parse OID and path from line
            line_buffer[buffer_pos] = '\0';

            // Find space separator between OID and path
            space_pos = 0;
            while (line_buffer[space_pos] && line_buffer[space_pos] != ' ')
                space_pos++;

            // Validate line format (must have OID space path)
            if (space_pos < 1 || space_pos >= buffer_pos - 1)
                ereport(FATAL, (errmsg("invalid data in file \"%s\"", TABLESPACE_MAP)));

            line_buffer[space_pos++] = '\0';  // Split OID and path

            // Step 4: Create tablespace entry
            tablespace_entry = palloc0(sizeof(tablespaceinfo));

            // Convert OID string to number
            char *endptr;
            tablespace_entry->oid = strtoul(line_buffer, &endptr, 10);
            if (*endptr != '\0' || errno == EINVAL || errno == ERANGE)
                ereport(FATAL, (errmsg("invalid data in file \"%s\"", TABLESPACE_MAP)));

            // Copy path string
            tablespace_entry->path = pstrdup(line_buffer + space_pos);

            // Add to output list
            *tablespaces = lappend(*tablespaces, tablespace_entry);

            buffer_pos = 0;
            continue;
        }

        // Handle backslash escaping
        if (!was_backslash && ch == '\\') {
            was_backslash = true;
        } else {
            // Add character to buffer
            if (buffer_pos < sizeof(line_buffer) - 1)
                line_buffer[buffer_pos++] = ch;
            was_backslash = false;
        }
    }

    // Step 5: Validate file ending and cleanup
    if (buffer_pos != 0 || was_backslash)
        ereport(FATAL, (errmsg("invalid data in file \"%s\"", TABLESPACE_MAP)));

    if (ferror(map_file) || FreeFile(map_file))
        ereport(FATAL, (errmsg("could not read file \"%s\"", TABLESPACE_MAP)));

    return true;  // Successfully parsed tablespace_map
}
```

Key simplifications made:
- Added descriptive variable names (map_file instead of lfp, buffer_pos instead of i)
- Organized logic into clear steps with comments
- Simplified complex nested conditions into clearer flow
- Removed some of the intermediate variables (n) by combining operations
- Added step-by-step comments explaining the main algorithm phases
- Consolidated error handling patterns while keeping essential checks
- Maintained the exact same functional behavior and error conditions