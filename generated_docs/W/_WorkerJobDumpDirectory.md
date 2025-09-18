# _WorkerJobDumpDirectory

## Location
[src/bin/pg_dump/pg_backup_directory.c:832-848](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_directory.c#L832-L848)

## Overview
A worker function executed in child processes during parallel backup operations for directory-format archives that handles the actual data dumping for a single TOC entry.

## Definition


## Detailed Description
This function is specifically designed for parallel backup operations in the pg_dump utility when using directory-format archives. It runs in child processes spawned during parallel backup and is responsible for dumping the actual data content for one Table of Contents (TOC) entry. The function acts as a wrapper that delegates the core data writing functionality to . It's designed with a simple success/failure model where any failure results in the child process terminating, which is then detected by the parent process.

## Parameters / Member Variables
- : Archive handle containing the backup context and configuration
- : TOC entry representing the database object whose data needs to be dumped

## Dependencies
- Functions called/Symbols referenced:
  -  - Core function that performs the actual data writing
  -  - Type definition for table of contents entries
- Called from (representative examples):
  - Referenced in  structure initialization
  - Set up by  as part of directory format handler registration

## Notes and Other Information
- This function always returns 0 on success, indicating successful completion
- Error handling relies on process termination rather than return codes - failures cause the child process to die unexpectedly
- Part of the parallel backup infrastructure specific to directory-format archives
- The function comment indicates it "returns void" conceptually, as the return value is primarily for successful completion indication
- Designed for execution in forked child processes during parallel operations