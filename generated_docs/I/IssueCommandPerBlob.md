# IssueCommandPerBlob

## Location
[src/bin/pg_dump/pg_backup_db.c:552-598](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_db.c#L552-L598)

## Overview
IssueCommandPerBlob is a utility function in pg_dump that executes SQL commands for each large object (blob) OID listed in a TocEntry, with transaction management support for batch processing.

## Definition

```c
void
IssueCommandPerBlob(ArchiveHandle *AH, TocEntry *te,
					const char *cmdBegin, const char *cmdEnd)
```
## Detailed Description
This function processes a TocEntry containing large object OIDs (one per line) and wraps each OID in the provided SQL command fragments to generate complete SQL commands. It supports PostgreSQL's --transaction-size mode by tracking the number of commands issued and automatically committing/starting new transactions when the specified transaction size limit is reached.

The function parses the TocEntry's definition string line by line, treating each line as a large object OID, and constructs SQL commands using the format: . This pattern is commonly used for operations like granting/revoking permissions on large objects or performing other bulk operations.

## Parameters / Member Variables
- `*AH`: Archive handle containing database connection and restore options
- `*te`: TocEntry containing the definition string with large object OIDs (one per line)
- `*cmdBegin`: SQL command prefix to prepend to each OID
- `*cmdEnd`: SQL command suffix to append to each OID
## Dependencies
- Functions called/Symbols referenced:
  - [pg_strdup](../p/pg_strdup.md)
  - strchr
  - [ahprintf](../a/ahprintf.md)
  - [CommitTransaction](../C/CommitTransaction.md)
  - [StartTransaction](../S/StartTransaction.md)
  - [pg_free](../p/pg_free.md)
- Types referenced:
  - [ArchiveHandle](../A/ArchiveHandle.md)
  - [TocEntry](../T/TocEntry.md)
  - [RestoreOptions](../R/RestoreOptions.md)

- Called from (representative examples):
  - [RestoreArchive](../R/RestoreArchive.md)
  - [_printTocEntry](../p/_printTocEntry.md)
  - [IssueACLPerBlob](IssueACLPerBlob.md)

## Notes and Other Information
- This function is specific to pg_dump/pg_restore functionality and handles large object management
- The transaction size feature allows for better performance and memory management when processing large numbers of large objects
- When a database connection exists, actual CommitTransaction/StartTransaction calls are made; otherwise, SQL COMMIT/BEGIN statements are output
- The function assumes the TocEntry's defn field contains newline-separated large object OIDs
- Each generated command is automatically terminated with a semicolon
- File location: src/bin/pg_dump/pg_backup_db.c:552-598

## Simplified Source

```c
void IssueCommandPerBlob(ArchiveHandle *AH, TocEntry *te,
                         const char *cmdBegin, const char *cmdEnd) {
    // Create working copy of blob OIDs definition string
    char *buf = pg_strdup(te->defn);
    RestoreOptions *ropt = AH->public.ropt;
    char *st = buf;
    char *en;

    // Process each line (blob OID) in the definition
    while ((en = strchr(st, '\n')) != NULL) {
        *en++ = '\0';

        // Generate command: cmdBegin + OID + cmdEnd
        ahprintf(AH, "%s%s%s;\n", cmdBegin, st, cmdEnd);

        // Handle transaction size limits for batch processing
        if (ropt && ropt->txn_size > 0) {
            if (++AH->txnCount >= ropt->txn_size) {
                if (AH->connection) {
                    CommitTransaction(&AH->public);
                    StartTransaction(&AH->public);
                } else {
                    ahprintf(AH, "COMMIT;\nBEGIN;\n\n");
                }
                AH->txnCount = 0;
            }
        }

        st = en;
    }

    ahprintf(AH, "\n");
    pg_free(buf);
}
```