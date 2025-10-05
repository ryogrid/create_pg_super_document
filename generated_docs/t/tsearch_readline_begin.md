# tsearch_readline_begin

## Location
[src/backend/tsearch/ts_locale.c:134-156](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/ts_locale.c#L134-L156)

## Overview
Initializes a tsearch_readline_state structure to read a file line by line with enhanced error reporting context for text search configuration files.

## Definition

```c
bool
tsearch_readline_begin(tsearch_readline_state *stp,
					   const char *filename)
```
## Detailed Description
This function sets up the infrastructure for reading text search configuration files (like stop word files, dictionaries, etc.) with better error handling than direct file reading. It opens the specified file, initializes the state structure, and sets up an error context callback that provides line number information when errors occur during subsequent reading operations.

The function is designed to be used as part of a three-function sequence: tsearch_readline_begin() to initialize, tsearch_readline() to read lines, and tsearch_readline_end() to clean up.

## Parameters / Member Variables
- `*stp`: Pointer to tsearch_readline_state structure that will be initialized for reading operations
- `*filename`: Path to the file to be opened; this string must remain valid until tsearch_readline_end() is called
## Dependencies
- Functions called/Symbols referenced:
  - [AllocateFile](../A/AllocateFile.md)
  - [initStringInfo](../i/initStringInfo.md)
  - [tsearch_readline_callback](tsearch_readline_callback.md)
  - [tsearch_readline_state](tsearch_readline_state.md) (struct type)
- Called from (representative examples):
  - [dsynonym_init](../d/dsynonym_init.md)
  - [thesaurusRead](thesaurusRead.md)
  - [NIImportDictionary](../N/NIImportDictionary.md)
  - [NIImportOOAffixes](../N/NIImportOOAffixes.md)
  - [NIImportAffixes](../N/NIImportAffixes.md)
  - [readstoplist](../r/readstoplist.md)

## Notes and Other Information
- Returns true on successful file opening, false on failure
- The caller is responsible for providing custom ereport() messages for file open failures
- Sets up error context stack to provide meaningful error messages with line numbers
- The filename parameter must remain valid throughout the entire reading session
- Typical usage pattern involves opening file, reading lines in a loop, then calling cleanup function
- Used primarily for reading text search configuration files in PostgreSQL's full-text search system

## Simplified Source

```c
bool tsearch_readline_begin(tsearch_readline_state *stp, const char *filename) {
    // Try to open file for reading
    if ((stp->fp = AllocateFile(filename, "r")) == NULL)
        return false;

    // Initialize state structure
    stp->filename = filename;
    stp->lineno = 0;
    initStringInfo(&stp->buf);
    stp->curline = NULL;

    // Set up error context for better error reporting
    stp->cb.callback = tsearch_readline_callback;
    stp->cb.arg = (void *) stp;
    stp->cb.previous = error_context_stack;
    error_context_stack = &stp->cb;

    return true;
}
```