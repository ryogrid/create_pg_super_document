# EndLO

## Location
src/bin/pg_dump/pg_backup_archiver.c: 1407 - 1424

## Overview
EndLO signals the completion of archival for a PostgreSQL Large Object (LO), marking the end of LO data output within the current dump context.

## Definition


## Detailed Description
EndLO completes the large object archival process initiated by StartLO, serving as the closing bracket for large object dumping operations. This function provides a format-agnostic interface for signaling that all data for a specific large object has been written to the archive. It works in conjunction with StartLO to properly encapsulate large object data within the archive format.

Unlike StartLO, EndLO performs a conditional check for the EndLOPtr function pointer before calling the format-specific handler. This design accommodates archive formats where end-of-LO processing is optional or not required. When the format-specific handler is available, it is called with the current TOC entry context and the OID of the large object being completed.

The function maintains consistency with the large object archival protocol by operating within the same TOC entry context established during StartLO. This ensures proper association between the large object data and its metadata throughout the dump process.

## Parameters / Member Variables
- : Archive pointer representing the current dump session
- : Object identifier (OID) of the large object being completed

## Dependencies
- Functions called/Symbols referenced:
  - AH->EndLOPtr (format-specific large object end handler, called conditionally)
- Called from (representative examples):
  - [dumpLOs](../d/dumpLOs.md)

## Notes and Other Information
- This function complements StartLO to provide complete large object archival bracketing
- [EndLO](EndLO.md) processing is optional for some archive formats (conditional EndLOPtr check)
- Must be called within the context of the same TOC entry used for the corresponding StartLO call
- Always returns 1 to indicate successful completion, unlike StartLO which can terminate on error
- Part of the Large Object Archival subsystem in pg_dump
- The function gracefully handles formats that don't require explicit end-of-LO processing
- Proper pairing with StartLO is essential for maintaining archive format integrity
- Format-specific implementations may use this signal for finalizing LO data, updating metadata, or cleaning up resources