# SplitGUCList

## Location
[src/bin/pg_dump/dumputils.c:761-860](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/dumputils.c#L761-L860)

## Overview
SplitGUCList is a utility function that parses a string containing identifiers or file names, specifically designed for splitting the value of a GUC_LIST_QUOTE GUC (Grand Unified Configuration) variable.

## Definition

```c
bool
SplitGUCList(char *rawstring, char separator,
			 char ***namelist)
```
## Detailed Description
This function splits a delimited string into a list of individual components without presuming whether the elements will be treated as identifiers or file names. The function is designed to work with strings that have already been processed through flatten_set_variable_args(), so it never performs downcasing or truncation operations.

The function handles both quoted and unquoted elements:
- Quoted elements can contain any characters and handle quote-quote pairs (doubled quotes are collapsed into single quotes)
- Unquoted elements extend until a separator or whitespace is encountered
- Embedded whitespace is disallowed for simplicity, as it should have led to double-quoting during input processing

The function modifies the input string in-place to contain the separated identifiers and returns a list of pointers into the modified string.

## Parameters / Member Variables
- `*rawstring`: The input string that must be overwritable. On return, it's been modified to contain the separated identifiers with null terminators
- `separator`: The separator punctuation expected between identifiers (typically '.' or ','). Whitespace may also appear around identifiers
- `***namelist`: Output parameter filled with a palloc'd list of pointers to identifiers within the modified rawstring. Caller should list_free() this even on error return
## Dependencies
- Functions called/Symbols referenced:
  - [scanner_isspace](../s/scanner_isspace.md): Used to skip whitespace characters
  - strchr: Used to find closing quotes in quoted strings
  - memmove: Used to collapse adjacent quotes
  - strlen: Used for string length calculation
  - [lappend](../l/lappend.md): Used to add elements to the output list

- Called from (representative examples):
  - [parse_hba_auth_opt](../p/parse_hba_auth_opt.md): Used in HBA (Host-Based Authentication) configuration parsing
  - [PostmasterMain](../P/PostmasterMain.md): Used in postmaster initialization
  - [check_debug_io_direct](../c/check_debug_io_direct.md): Used in file descriptor management
  - [pg_get_functiondef](../p/pg_get_functiondef.md): Used in rule utilities for function definitions
  - [makeAlterConfigCommand](../m/makeAlterConfigCommand.md): Used in pg_dump utilities
  - [dumpFunc](../d/dumpFunc.md): Used in pg_dump for function dumping

## Notes and Other Information
- The function returns true if parsing is successful, false if there is a syntax error
- Empty strings are allowed and return true with an empty namelist
- There is a duplicate version of this function in src/bin/pg_dump/dumputils.c that should be kept in sync
- The API is intentionally identical to SplitIdentifierString for consistency
- The function is part of the varlena.c module which handles variable-length data types
- Located at src/backend/utils/adt/varlena.c:3705-3793

## Simplified Source

```c
// Simplified version of SplitGUCList
bool SplitGUCList(char *rawstring, char separator, List **namelist) {
    char *nextp = rawstring;
    bool done = false;

    *namelist = NIL;

    // Skip leading whitespace
    while (scanner_isspace(*nextp))
        nextp++;

    // Allow empty string
    if (*nextp == '\0')
        return true;

    // Main parsing loop - process each identifier
    do {
        char *curname;
        char *endp;

        if (*nextp == '"') {
            // Handle quoted identifier: "name" or "name""with""quotes"
            curname = nextp + 1;

            // Find end quote, handling doubled quotes
            for (;;) {
                endp = strchr(nextp + 1, '"');
                if (endp == NULL)
                    return false;  // Missing closing quote

                if (endp[1] != '"')
                    break;  // Found actual end

                // Collapse doubled quotes: "" becomes "
                memmove(endp, endp + 1, strlen(endp));
                nextp = endp;
            }
            nextp = endp + 1;
        } else {
            // Handle unquoted identifier: extends to separator or whitespace
            curname = nextp;
            while (*nextp && *nextp != separator && !scanner_isspace(*nextp))
                nextp++;
            endp = nextp;

            if (curname == nextp)
                return false;  // Empty identifier not allowed
        }

        // Skip whitespace after identifier
        while (scanner_isspace(*nextp))
            nextp++;

        // Check what comes next
        if (*nextp == separator) {
            nextp++;
            while (scanner_isspace(*nextp))
                nextp++;  // Skip whitespace after separator
            // More identifiers expected
        } else if (*nextp == '\0') {
            done = true;  // End of string
        } else {
            return false;  // Invalid character
        }

        // Terminate current identifier and add to list
        *endp = '\0';
        *namelist = lappend(*namelist, curname);

    } while (!done);

    return true;
}
```

Key simplifications made:
- Consolidated whitespace skipping into clear sections
- Added descriptive comments for the main logic blocks
- Simplified the quote handling explanation
- Streamlined the identifier parsing flow
- Made the loop termination conditions more explicit
- Removed some intermediate variable assignments for clarity
- Focused on the core parsing algorithm rather than low-level details