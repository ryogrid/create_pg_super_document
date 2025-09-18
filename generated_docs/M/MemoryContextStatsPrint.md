# MemoryContextStatsPrint

## Location
[src/backend/utils/mmgr/mcxt.c:973-1051](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/mcxt.c#L973-L1051)

## Overview
MemoryContextStatsPrint is a callback function used by MemoryContextStatsInternal to format and output individual memory context statistics with proper indentation and identifier handling.

## Definition
```c
static void MemoryContextStatsPrint(MemoryContext context, void *passthru,
                                    const char *stats_string,
                                    bool print_to_stderr)
```

## Detailed Description
This static callback function handles the formatting and output of memory context statistics for individual contexts within the hierarchy. It implements several sophisticated features for clean output presentation: intelligent handling of dynahash contexts by using the hash table name instead of the generic "dynahash" label, truncation and sanitization of long identifiers (such as SQL queries) to prevent output overflow, replacement of ASCII control characters with spaces, and proper Unicode-aware truncation using pg_mbcliplen. The function provides hierarchical indentation based on the context level and supports both stderr and logging system output modes.

## Parameters / Member Variables
- `context`: The memory context whose statistics are being printed
- `passthru`: Void pointer containing the current hierarchy level (cast to int *)
- `stats_string`: Pre-formatted statistics string from the context-specific stats method
- `print_to_stderr`: If true, output to stderr; if false, use ereport logging system

## Dependencies
- Functions called/Symbols referenced:
  - [pg_mbcliplen](../p/pg_mbcliplen.md) (Unicode-aware string truncation)
  - LOG_SERVER_ONLY (logging level constant)
  - [errhidestmt](../e/errhidestmt.md)/errhidecontext (error reporting functions)
  - Various string manipulation functions (strcmp, strcpy, strlen, strcat)
- Called from (representative examples):
  - [MemoryContextStatsInternal](MemoryContextStatsInternal.md) (as a callback via context->methods->stats)

## Notes and Other Information
- Static function, only accessible within mcxt.c
- Serves as a callback function passed to context->methods->stats implementations
- Special handling for dynahash contexts to use hash table name for cleaner output
- Truncates identifiers at 100 bytes with Unicode-awareness to prevent display issues
- Replaces control characters (like newlines) with spaces in identifiers
- Provides hierarchical indentation using the level parameter from passthru
- Uses "..." suffix to indicate when identifiers have been truncated
- Critical for readable memory context debugging output
- Supports both stderr output (with indentation) and server logging (with level prefixes)