# pchomp

## Location
src/backend/utils/mmgr/mcxt.c: 1723 - 1731

## Overview
A utility function that creates a copy of a string with all trailing newline characters removed, similar to Perl's chomp function.

## Definition


## Detailed Description
pchomp removes trailing newline characters from a string and returns a newly allocated copy without those characters. The function works by first determining the length of the input string, then scanning backwards from the end to find all trailing newline characters ('\n'). It then uses pnstrdup to create a copy of the string up to (but not including) the trailing newlines.

This function is particularly useful for processing text data where trailing newlines need to be removed, such as when reading lines from files or processing command output. The function handles multiple consecutive trailing newlines by removing all of them.

## Parameters / Member Variables
- `in`: The null-terminated input string from which trailing newlines should be removed

## Dependencies
- Functions called/Symbols referenced:
  - strlen
  - [pnstrdup](pnstrdup.md)
- Called from (representative examples):
  - [libpqrcv_connect](../l/libpqrcv_connect.md)
  - [libpqrcv_identify_system](../l/libpqrcv_identify_system.md)
  - [libpqrcv_startstreaming](../l/libpqrcv_startstreaming.md)
  - [libpqrcv_endstreaming](../l/libpqrcv_endstreaming.md)
  - [libpqrcv_receive](../l/libpqrcv_receive.md)
  - [libpqrcv_exec](../l/libpqrcv_exec.md)

## Notes and Other Information
- The function removes ALL trailing newline characters, not just the last one
- Returns a newly allocated string in the current memory context
- Commonly used in replication and libpq-related code for cleaning up command responses
- If the input string contains no trailing newlines, a complete copy is still made
- The name 'pchomp' follows the PostgreSQL naming convention (p-prefix) and references Perl's chomp function
- Located in src/backend/utils/mmgr/mcxt.c at lines 1723-1731
- Primarily used in PostgreSQL's replication subsystem for processing libpq command responses