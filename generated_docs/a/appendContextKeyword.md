# appendContextKeyword

## Location
src/backend/utils/adt/ruleutils.c: 8786 - 8839

## Overview
Appends a keyword to the output buffer with proper indentation formatting when pretty printing is enabled for PostgreSQL rule decompilation.

## Definition

```c
static void
appendContextKeyword(deparse_context *context, const char *str,
					 int indentBefore, int indentAfter, int indentPlus)
```
## Detailed Description
This function is a core component of PostgreSQL's rule decompilation system that formats SQL output with proper indentation. When pretty printing is enabled (PRETTY_INDENT), it performs sophisticated indentation management including removing trailing spaces, adding newlines, and calculating appropriate indentation levels. The function implements a scaling mechanism to prevent unbounded indentation growth in deeply nested SQL structures, ensuring O(N) rather than O(N^2) whitespace usage.

The indentation algorithm includes a wraparound mechanism when indentation exceeds PRETTYINDENT_LIMIT to maintain readability and prevent excessive horizontal space usage.

## Parameters / Member Variables
- : Deparse context containing the output buffer and current indentation state
- : The keyword string to append to the output buffer  
- : Amount to adjust indentation level before appending the keyword
- : Amount to adjust indentation level after appending the keyword
- : Additional indentation to add for this specific keyword

## Dependencies
- Functions called/Symbols referenced:
  - PRETTY_INDENT (macro to check if pretty printing is enabled)
  - removeStringInfoSpaces (removes trailing spaces from buffer)
  - appendStringInfoChar (appends single character to buffer)
  - appendStringInfoSpaces (appends specified number of spaces)
  - appendStringInfoString (appends string to buffer)
  - PRETTYINDENT_LIMIT (constant defining indentation limit)
  - PRETTYINDENT_STD (standard indentation amount)
- Called from (representative examples):
  - get_select_query_def (for SELECT query formatting)
  - get_insert_query_def (for INSERT query formatting)
  - get_update_query_def (for UPDATE query formatting)
  - get_delete_query_def (for DELETE query formatting)
  - get_merge_query_def (for MERGE query formatting)
  - get_from_clause (for FROM clause formatting)

## Notes and Other Information
- This is a static function within ruleutils.c, used exclusively for SQL rule decompilation
- The scaling algorithm prevents excessive indentation in deeply nested queries by using a reduction factor
- When pretty printing is disabled, the function simply appends the keyword without any formatting
- The function maintains the invariant that indentLevel never goes negative by clamping to 0
- Location: src/backend/utils/adt/ruleutils.c:8786-8839