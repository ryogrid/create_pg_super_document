# IssueACLPerBlob

## Location
[src/bin/pg_dump/pg_backup_db.c:599-672](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_db.c#L599-L672)

## Overview
IssueACLPerBlob processes "LARGE OBJECTS" ACL TocEntries by parsing GRANT/REVOKE commands and applying them to all large objects listed in the associated BLOB METADATA entry.

## Definition

```c
void
IssueACLPerBlob(ArchiveHandle *AH, TocEntry *te)
```
## Detailed Description
This function handles the restoration of Access Control List (ACL) permissions for large objects in pg_dump/pg_restore. To optimize dump file size, the TocEntry contains only one copy of the GRANT/REVOKE commands written for the first blob in a group. The function expands these commands to apply to all large objects listed in the corresponding BLOB METADATA entry.

The function performs sophisticated parsing of SQL commands to extract the structure:
1. Locates the "LARGE OBJECT" keyword to separate command prefix from suffix
2. Handles double-quoted role names by tracking quote state
3. Splits commands at semicolons to process each command separately
4. Calls IssueCommandPerBlob to apply each parsed command to all relevant large objects

This approach ensures that ACL commands are efficiently stored while being correctly applied to all target large objects during restoration.

## Parameters / Member Variables
- `*AH`: Archive handle containing database connection and restoration context
- `*te`: TocEntry containing the ACL commands to be processed (depends on a BLOB METADATA entry)
## Dependencies
- Functions called/Symbols referenced:
  - [getTocEntryByDumpId](../g/getTocEntryByDumpId.md)
  - [pg_strdup](../p/pg_strdup.md)
  - strncmp
  - isdigit
  - isspace
  - [IssueCommandPerBlob](IssueCommandPerBlob.md)
  - [pg_free](../p/pg_free.md)
- Types referenced:
  - [ArchiveHandle](../A/ArchiveHandle.md)
  - [TocEntry](../T/TocEntry.md)

- Called from (representative examples):
  - [_printTocEntry](../p/_printTocEntry.md)

## Notes and Other Information
- This function specifically handles "LARGE OBJECTS" ACL TocEntries and depends on the first dependency being a "BLOB METADATA" entry
- The parsing logic assumes well-formed SQL commands and handles double-quoted identifiers correctly
- Commands are processed in the order they appear, with updates to blobs being interleaved if multiple commands exist
- The function uses the first dependency of the TocEntry to locate the associated BLOB METADATA entry
- Error handling includes fatal errors if the required BLOB METADATA entry cannot be found
- File location: src/bin/pg_dump/pg_backup_db.c:599-672

## Simplified Source

```c
void IssueACLPerBlob(ArchiveHandle *AH, TocEntry *te) {
    // Get the associated BLOB METADATA entry
    TocEntry *blobte = getTocEntryByDumpId(AH, te->dependencies[0]);
    if (!blobte) {
        pg_fatal("could not find entry for ID %d", te->dependencies[0]);
    }

    // Create working copy of ACL commands
    char *buf = pg_strdup(te->defn);
    char *st = buf;
    char *en = buf;
    char *st2 = NULL;
    bool inquotes = false;

    // Parse ACL commands to extract prefix/suffix around "LARGE OBJECT"
    while (*en) {
        // Track double-quoted strings to avoid parsing quoted content
        if (*en == '"') {
            inquotes = !inquotes;
        }

        if (!inquotes) {
            // Found "LARGE OBJECT" - this splits command into prefix/suffix
            if (strncmp(en, "LARGE OBJECT ", 13) == 0) {
                en += 13;
                *en++ = '\0';  // Terminate first half

                // Skip the blob OID digits
                while (isdigit(*en)) en++;

                st2 = en;  // Second half starts here
            }
            // Found semicolon - end of command
            else if (*en == ';') {
                *en++ = '\0';  // Terminate second half

                // Apply this command pattern to all blobs
                IssueCommandPerBlob(AH, blobte, st, st2);

                // Skip whitespace and reset for next command
                while (isspace(*en)) en++;
                st = en;
                st2 = NULL;
            }
            else {
                en++;
            }
        } else {
            en++;
        }
    }

    pg_free(buf);
}
```