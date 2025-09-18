# PQgetline

## Location
src/interfaces/libpq/fe-exec.c: 2854 - 2900

## Overview
Legacy function for reading newline-terminated strings from PostgreSQL backend during COPY operations, now deprecated due to inability to handle binary data.

## Definition


## Detailed Description
PQgetline is a deprecated libpq function originally designed for reading text data from COPY TO STDOUT operations. It reads data line by line, similar to fgets(3), but strips the terminating newline character like gets(3). The function was designed for the older text-based COPY protocol and cannot handle binary data safely.

The function implements several important behaviors:
- Reads up to length-1 characters to ensure null termination
- Strips trailing newline characters from received data
- Requires caller to detect the end-of-copy signal (a line containing just "\.")
- Returns EOF for binary COPY operations or error conditions
- Provides backward compatibility with older PostgreSQL client applications

**IMPORTANT**: This function is deprecated because it cannot handle binary data properly. Modern applications should use PQgetCopyData instead.

## Parameters / Member Variables
- : PostgreSQL connection handle for the COPY operation
- : Character buffer to receive the line data (must be pre-allocated)
- : Maximum number of characters to read (including null terminator)

## Dependencies
- Functions called/Symbols referenced:
  - pqGetline3
- Called from (representative examples):
  - Legacy applications using old COPY protocol
  - Referenced in libpq-fe.h for API completeness

## Notes and Other Information
- Returns EOF on error or invalid arguments
- Returns 0 if end-of-line reached (newline character found)
- Returns 1 if buffer filled before newline found
- Minimum buffer length of 3 required to hold the end-of-copy terminator "\.\n"
- Buffer is null-terminated in all cases
- **DEPRECATED**: Cannot handle binary data safely
- Caller must detect end-of-copy signal manually (line containing only "\.")
- Modern applications should use PQgetCopyData for all COPY operations
- Maintained for backward compatibility with legacy code
- Does not work with COPY BINARY operations (returns EOF)