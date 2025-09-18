# PageXLogRecPtrGet

## Location
[src/include/storage/bufpage.h:101-105](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/bufpage.h#L101-L105)

## Overview
PageXLogRecPtrGet is an inline function that converts a PageXLogRecPtr structure to a 64-bit XLogRecPtr value, combining the high and low 32-bit components into a single WAL record pointer.

## Definition


## Detailed Description
This function takes a PageXLogRecPtr structure (which contains separate 32-bit xlogid and xrecoff fields) and combines them into a single 64-bit XLogRecPtr value. The conversion is performed by shifting the xlogid (high bits) left by 32 positions and ORing it with the xrecoff (low bits). This is part of PostgreSQL's Write-Ahead Logging (WAL) system for tracking log sequence numbers on pages.

## Parameters / Member Variables
- : A PageXLogRecPtr structure containing:
  - : The high 32 bits of the WAL record pointer
  - : The low 32 bits of the WAL record pointer

## Dependencies
- Functions called/Symbols referenced:
  - PageXLogRecPtr (structure type)
  - XLogRecPtr (return type - 64-bit WAL pointer)
- Called from (representative examples):
  - GistPageGetNSN (in src/include/access/gist.h:186)
  - [PageGetLSN](PageGetLSN.md) (in src/include/storage/bufpage.h:386)

## Notes and Other Information
- This is an inline function defined in bufpage.h for performance
- The function performs a simple bit manipulation to reconstruct a 64-bit WAL pointer from its 32-bit components
- WAL pointers are used throughout PostgreSQL to track the location of log records for crash recovery and replication
- The PageXLogRecPtr structure format allows for compatibility with systems that may need to store WAL pointers in a split format