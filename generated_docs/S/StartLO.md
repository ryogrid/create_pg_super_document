# StartLO

## Location
src/bin/pg_dump/pg_backup_archiver.c: 1393 - 1406

## Overview
StartLO initiates the archival process for a PostgreSQL Large Object (LO), signaling the beginning of LO data output within the current dump context.

## Definition


## Detailed Description
StartLO is part of PostgreSQL's large object archival system that manages the dumping of binary large objects (BLOBs) stored in the database. This function serves as the entry point for beginning the output of a specific large object during the dump process. It acts as a format-agnostic interface that delegates the actual implementation to format-specific handlers.

The function performs essential validation to ensure that the chosen archive format supports large object output. If the format does not support large objects (indicated by a NULL StartLOPtr function pointer), the function terminates with a fatal error. For supported formats, it delegates to the format-specific implementation, passing the current TOC entry context and the OID of the large object to be processed.

The function is designed to work within the context of a data dumper routine, where AH->currToc points to the current table of contents entry being processed. This ensures that large object dumping occurs within the proper archival context and can be properly associated with the appropriate metadata.

## Parameters / Member Variables
- : Archive pointer representing the current dump session
- : Object identifier (OID) of the large object to begin archiving

## Dependencies
- Functions called/Symbols referenced:
  - pg_fatal (for error handling when format doesn't support LOs)
  - AH->StartLOPtr (format-specific large object start handler)
- Called from (representative examples):
  - dumpLOs

## Notes and Other Information
- This function is part of the Large Object Archival subsystem in pg_dump
- Not all archive formats support large object output - the function validates this capability
- Must be called within the context of an active TOC entry (AH->currToc must be valid)
- Returns 1 on successful initiation, but terminates the program on format incompatibility
- Part of a paired operation with EndLO to bracket large object data output
- The actual large object data writing occurs between StartLO and EndLO calls
- Format-specific implementations handle the details of how large objects are stored in each archive format