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

## Simplified Source

```c
static void
MemoryContextStatsPrint(MemoryContext context, void *passthru,
                        const char *stats_string,
                        bool print_to_stderr)
{
    int level = *(int *) passthru;
    const char *name = context->name;
    const char *ident = context->ident;
    char truncated_ident[110];

    // Special case: use hash table name for dynahash contexts
    if (ident && strcmp(name, "dynahash") == 0)
    {
        name = ident;
        ident = NULL;
    }

    truncated_ident[0] = '\0';

    // Process identifier if present
    if (ident)
    {
        int idlen = strlen(ident);
        bool truncated = false;

        strcpy(truncated_ident, ": ");
        int i = strlen(truncated_ident);

        // Truncate long identifiers (>100 chars) with Unicode awareness
        if (idlen > 100)
        {
            idlen = pg_mbcliplen(ident, idlen, 100);
            truncated = true;
        }

        // Copy identifier, replacing control characters with spaces
        while (idlen-- > 0)
        {
            unsigned char c = *ident++;
            if (c < ' ')
                c = ' ';
            truncated_ident[i++] = c;
        }
        truncated_ident[i] = '\0';

        // Add truncation indicator if needed
        if (truncated)
            strcat(truncated_ident, "...");
    }

    // Output formatted stats
    if (print_to_stderr)
    {
        // Print with indentation to stderr
        for (int i = 0; i < level; i++)
            fprintf(stderr, "  ");
        fprintf(stderr, "%s: %s%s\n", name, stats_string, truncated_ident);
    }
    else
    {
        // Log to server log with level information
        ereport(LOG_SERVER_ONLY,
                (errhidestmt(true),
                 errhidecontext(true),
                 errmsg_internal("level: %d; %s: %s%s",
                                level, name, stats_string, truncated_ident)));
    }
}
```