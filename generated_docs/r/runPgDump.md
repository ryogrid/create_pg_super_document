# runPgDump

## Location
src/bin/pg_dump/pg_dumpall.c: 1677 - 1730

## Overview
Executes the pg_dump utility as a subprocess with specified database name and creation options, constructing the appropriate command line and connection parameters.

## Definition


## Detailed Description
This function serves as a wrapper to execute the pg_dump utility as a system command from within pg_dumpall. It constructs a complete pg_dump command line by combining the pg_dump binary path, global dump options, creation options, output format specifications, and database connection parameters. The function handles two output formats: plain-append format (-Fa) when writing to a file, and plain format (-Fp) for standard output.

The function builds a connection string by appending the target database name to the existing connection string stem, properly escaping the database name for shell safety. After constructing the complete command, it logs the command being executed and runs it via the system() call, returning the exit status for error handling by the caller.

## Parameters / Member Variables
- : Name of the database to dump using pg_dump
- : Additional command-line options for pg_dump (e.g., "--clean --create")

## Dependencies
- Functions called/Symbols referenced:
  - [PQExpBufferData](../P/PQExpBufferData.md) (buffer data structure)
  - initPQExpBuffer (buffer initialization)
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md) (formatted buffer output)
  - [appendConnStrVal](../a/appendConnStrVal.md) (connection string value appending)
  - [appendShellString](../a/appendShellString.md) (shell-safe string appending)
  - pg_log_info (logging utility)
  - system (system command execution)
  - termPQExpBuffer (buffer cleanup)
- Called from (representative examples):
  - [dumpDatabases](../d/dumpDatabases.md) (in pg_dumpall.c at line 1655)

## Notes and Other Information
- Returns the exit status from the system() call, allowing caller to detect pg_dump failures
- Uses undocumented plain-append format (-Fa) when writing to files for proper append behavior
- Properly escapes database names and shell strings to prevent injection attacks
- Flushes output streams before executing system command to ensure proper log ordering
- Relies on global variables like pg_dump_bin, pgdumpopts, filename, and connstr
- The connection string construction assumes a properly formatted base connection string
- [Command](../C/Command.md) execution is synchronous - function blocks until pg_dump completes