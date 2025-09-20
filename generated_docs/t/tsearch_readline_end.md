# tsearch_readline_end

## Location
[src/backend/tsearch/ts_locale.c:202-224](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/ts_locale.c#L202-L224)

## Overview
Cleans up resources and closes a file that was being read using the tsearch_readline facility.

## Definition

```c
void
tsearch_readline_end(tsearch_readline_state *stp)
```
## Detailed Description
This function performs cleanup operations after reading a text search configuration file using tsearch_readline(). It releases all allocated memory, closes the file handle, and properly restores the error context stack. This function should always be called after finishing with a file that was opened using tsearch_readline_begin(), even if errors occurred during reading.

The function carefully manages memory by freeing the current line buffer (if it was separately allocated), the internal string buffer, and closing the file handle. It also removes the error context callback from the error context stack.

## Parameters / Member Variables
- : Pointer to the tsearch_readline_state structure that was used for reading operations

## Dependencies
- Functions called/Symbols referenced:
  - [pfree](../p/pfree.md)
  - FreeFile
  - error_context_stack (global variable)
- Called from (representative examples):
  - [dsynonym_init](../d/dsynonym_init.md)
  - [thesaurusRead](thesaurusRead.md)
  - [NIImportDictionary](../N/NIImportDictionary.md)
  - NIImportOOAffixes
  - NIImportAffixes
  - [readstoplist](../r/readstoplist.md)

## Notes and Other Information
- Must be called to properly clean up resources after tsearch_readline operations
- Should be called even if errors occurred during file reading
- Handles cases where curline may or may not be separately allocated from buf.data
- Restores the error context stack to its previous state
- Part of the three-function sequence: tsearch_readline_begin(), tsearch_readline(), tsearch_readline_end()
- Does not return any value (void function)
- Essential for preventing memory leaks in text search file processing