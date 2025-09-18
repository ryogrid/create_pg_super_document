# tsearch_readline_state

## Location
[src/include/tsearch/ts_locale.h:33-34](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/tsearch/ts_locale.h#L33-L34)

## Overview
A working state structure for managing file reading operations in PostgreSQL's text search (tsearch) system, designed to safely read and process configuration files with proper encoding conversion and error handling.

## Definition


## Detailed Description
The  structure provides a complete state management system for reading text search configuration files line by line. It handles UTF-8 input validation, encoding conversion to the database's encoding, memory management, and comprehensive error reporting with file context information. This structure is designed to be used as a local variable in calling functions and provides safe, incremental file processing with proper resource cleanup.

The structure supports reading files that are expected to be in UTF-8 format and automatically converts them to the database encoding as needed. It maintains both the original UTF-8 data and the converted database-encoded version for different processing needs, while providing detailed error context including filename and line numbers for debugging purposes.

## Parameters / Member Variables
- : FILE pointer to the currently open configuration file being read
- : Pointer to the filename string for error reporting (must remain valid throughout the reading session)  
- : Current line number being processed (starts at 0, incremented with each read)
- : StringInfoData buffer containing the current input line in UTF-8 encoding
- : Pointer to the current input line in the database's encoding (may be NULL, equal to buf.data, or a separate palloc'd string)
- : ErrorContextCallback structure for providing detailed error context in case of failures

## Dependencies
- Functions called/Symbols referenced:
  - [StringInfoData](../S/StringInfoData.md) (from lib/stringinfo.h)
  - ErrorContextCallback (from utils/elog.h)
  - FILE (from standard C library)

- Called from (representative examples):
  - [tsearch_readline_begin](tsearch_readline_begin.md) (initializes the state)
  - [tsearch_readline](tsearch_readline.md) (reads next line using the state) 
  - [tsearch_readline_end](tsearch_readline_end.md) (cleans up the state)
  - [dsynonym_init](../d/dsynonym_init.md) (dictionary synonym initialization)
  - [thesaurusRead](thesaurusRead.md) (thesaurus dictionary reading)
  - [NIImportDictionary](../N/NIImportDictionary.md) (Ispell dictionary import)
  - [readstoplist](../r/readstoplist.md) (stop word list reading)

## Notes and Other Information
This structure must be used with the complete tsearch_readline family of functions: begin with , read lines with , and clean up with . The filename pointer passed to the begin function must remain valid throughout the entire reading session. The structure automatically manages encoding conversion from UTF-8 to database encoding and provides comprehensive error context for debugging configuration file issues. Memory management is handled automatically, but callers should always call the end function to ensure proper cleanup of file handles and allocated memory.