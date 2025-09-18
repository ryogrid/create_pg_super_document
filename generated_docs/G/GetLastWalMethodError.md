# GetLastWalMethodError

## Location
src/bin/pg_basebackup/walmethods.c: 1383 - 1388

## Overview
GetLastWalMethodError retrieves the last error message or error description from a WAL writing method, providing a human-readable error string for diagnostic purposes.

## Definition


## Detailed Description
This function provides a unified interface for accessing error information from WAL writing methods. It implements a two-tier error reporting system: if a specific error string has been set (lasterrstring), it returns that custom message; otherwise, it falls back to using the standard system error description for the stored errno value (lasterrno). This allows for both custom error messages and standard system error reporting within the same interface.

## Parameters / Member Variables
- : Pointer to the WalWriteMethod structure from which to retrieve error information

## Dependencies
- Functions called/Symbols referenced:
  - strerror (to convert errno to human-readable string)
  - WalWriteMethod (structure containing error state)
- Called from (representative examples):
  - mark_file_as_archived (in receivelog.c for error reporting)
  - open_walfile (in receivelog.c for file operation errors)
  - close_walfile (in receivelog.c for file closing errors)
  - ReceiveXlogStream (in receivelog.c for streaming errors)
  - ProcessKeepaliveMsg (in receivelog.c for keepalive message errors)
  - ProcessXLogDataMsg (in receivelog.c for WAL data processing errors)

## Notes and Other Information
- The function prioritizes custom error strings (lasterrstring) over errno-based messages (lasterrno)
- Returns a const char pointer that should not be modified or freed by the caller
- The returned string may point to static memory (from strerror) or method-managed memory (from lasterrstring)
- Used extensively throughout the WAL streaming and archiving code for error diagnostics
- Provides a consistent error reporting interface across all WAL method implementations (directory, tar, etc.)
- The error state is typically set by individual WAL method operations when they encounter failures