# EndDBCopyMode

## Location
[src/bin/pg_dump/pg_backup_db.c:500-528](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_db.c#L500-L528)

## Overview
Terminates a COPY operation during direct-to-database restore by properly ending the COPY state and returning libpq to its normal operational mode.

## Definition
void EndDBCopyMode(Archive *AHX, const char *tocEntryTag)

## Detailed Description
EndDBCopyMode handles the proper termination of COPY operations during database restoration. It calls PQputCopyEnd to signal the end of COPY data transmission, then retrieves and validates the final result status to ensure the operation completed successfully. The function includes comprehensive error checking and ensures that libpq is returned to an idle state by draining any remaining results. If the COPY operation fails, it reports the error with context about which table was being processed. The function also handles unexpected extra results with a warning, which helps diagnose potential protocol issues.

## Parameters / Member Variables
- `AHX`: Archive pointer (cast to ArchiveHandle internally) 
- `tocEntryTag`: String identifier for the table or object being processed, used in error messages

## Dependencies
- Functions called/Symbols referenced:
  - [PQputCopyEnd](../P/PQputCopyEnd.md)
  - [PQerrorMessage](../P/PQerrorMessage.md)
  - [pg_fatal](../p/pg_fatal.md)
  - [PQgetResult](../P/PQgetResult.md)
  - [PQresultStatus](../P/PQresultStatus.md)
  - [warn_or_exit_horribly](../w/warn_or_exit_horribly.md)
  - [PQclear](../P/PQclear.md)
  - pg_log_warning
- Constants referenced:
  - PGRES_COMMAND_OK
- Called from (representative examples):
  - [restore_toc_entry](../r/restore_toc_entry.md) (in pg_backup_archiver.c)

## Notes and Other Information
- Only performs operations if pgCopyIn flag is set, providing safe no-op behavior
- Sets pgCopyIn to false upon completion to prevent duplicate termination attempts
- Includes defensive programming by checking for unexpected extra results from libpq
- Part of the pg_backup_db.h interface for archive restoration operations
- Critical for proper cleanup of PostgreSQL COPY protocol state
- Error messages include table context to aid in debugging restoration issues

## Simplified Source

```c
void EndDBCopyMode(Archive *AHX, const char *tocEntryTag) {
    ArchiveHandle *AH = (ArchiveHandle *) AHX;

    if (AH->pgCopyIn) {
        // Signal end of COPY data transmission
        if (PQputCopyEnd(AH->connection, NULL) <= 0) {
            pg_fatal("error returned by PQputCopyEnd: %s",
                     PQerrorMessage(AH->connection));
        }

        // Check command status and ensure successful completion
        PGresult *res = PQgetResult(AH->connection);
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            warn_or_exit_horribly(AH, "COPY failed for table \"%s\": %s",
                                  tocEntryTag, PQerrorMessage(AH->connection));
        }
        PQclear(res);

        // Ensure libpq returns to idle state
        if (PQgetResult(AH->connection) != NULL) {
            pg_log_warning("unexpected extra results during COPY of table \"%s\"",
                           tocEntryTag);
        }

        AH->pgCopyIn = false;
    }
}
```