# parse_ident

## Location
[src/backend/utils/adt/misc.c:861-999](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/misc.c#L861-L999)

## Overview
A PostgreSQL function that parses a SQL qualified identifier string into separate identifier components, returning them as a text array.

## Definition
```c
Datum parse_ident(PG_FUNCTION_ARGS)
```

## Detailed Description
This function takes a qualified identifier string (e.g., "schema.table.column") and splits it into individual identifier components. It supports both quoted and unquoted identifiers according to SQL standards. The function handles proper identifier validation, including support for escaped double quotes within quoted identifiers. When strict mode is enabled via the second parameter, any characters after the last valid identifier cause an error. Unquoted identifiers are automatically downcased following PostgreSQL conventions, while quoted identifiers preserve their exact case.

The function processes identifiers by:
1. Skipping leading whitespace
2. Identifying quoted identifiers (enclosed in double quotes) or unquoted identifiers
3. Handling escaped quotes within quoted identifiers ("" becomes ")
4. Validating identifier characters using is_ident_start and is_ident_cont
5. Collecting valid identifiers into a text array
6. Supporting dot-separated qualified names

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - Argument 0: text - the qualified identifier string to parse
  - Argument 1: bool - strict mode flag; when true, disallows any characters after the last identifier

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TEXT_PP, PG_GETARG_BOOL, PG_RETURN_DATUM (PostgreSQL argument/return macros)
  - [text_to_cstring](../t/text_to_cstring.md)
  - [scanner_isspace](../s/scanner_isspace.md)
  - [is_ident_start](../i/is_ident_start.md)
  - [is_ident_cont](../i/is_ident_cont.md)
  - [downcase_identifier](../d/downcase_identifier.md)
  - [cstring_to_text_with_len](../c/cstring_to_text_with_len.md)
  - [accumArrayResult](../a/accumArrayResult.md)
  - [makeArrayResult](../m/makeArrayResult.md)
- Called from (representative examples):
  - No direct references found (likely called via PostgreSQL function mechanism)

## Notes and Other Information
- This is a PostgreSQL SQL-callable function (available as parse_ident in SQL)
- Supports SQL standard identifier syntax with proper quote handling
- Does not implicitly truncate long identifiers (unlike some PostgreSQL internal functions)
- Returns a text array where each element is one component of the qualified identifier
- Handles various error conditions with detailed error messages
- Preserves identifier length to allow user validation of identifier length limits
- Uses PostgreSQL's array building infrastructure for efficient result construction

## Simplified Source

```c
Datum parse_ident(PG_FUNCTION_ARGS) {
    text *qualname = PG_GETARG_TEXT_PP(0);
    bool strict = PG_GETARG_BOOL(1);
    char *qualname_str = text_to_cstring(qualname);
    ArrayBuildState *astate = NULL;
    char *nextp;
    bool after_dot = false;

    nextp = qualname_str;

    // Skip leading whitespace
    while (scanner_isspace(*nextp))
        nextp++;

    for (;;) {
        char *curname;
        bool missing_ident = true;

        if (*nextp == '"') {
            // Handle quoted identifier
            char *endp;
            curname = nextp + 1;

            // Find closing quote, handling escaped quotes ("")
            for (;;) {
                endp = strchr(nextp + 1, '"');
                if (endp == NULL)
                    ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("string is not a valid identifier: \"%s\"",
                                   text_to_cstring(qualname)),
                            errdetail("String has unclosed double quotes.")));

                if (endp[1] != '"')
                    break;  // Not an escaped quote

                // Remove one quote from escaped pair
                memmove(endp, endp + 1, strlen(endp));
                nextp = endp;
            }

            nextp = endp + 1;
            *endp = '\0';

            if (endp - curname == 0)
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("string is not a valid identifier: \"%s\"",
                               text_to_cstring(qualname)),
                        errdetail("Quoted identifier must not be empty.")));

            astate = accumArrayResult(astate, CStringGetTextDatum(curname),
                                    false, TEXTOID, CurrentMemoryContext);
            missing_ident = false;

        } else if (is_ident_start((unsigned char) *nextp)) {
            // Handle unquoted identifier
            char *downname;
            int len;
            text *part;

            curname = nextp++;
            while (is_ident_cont((unsigned char) *nextp))
                nextp++;

            len = nextp - curname;

            // Downcase unquoted identifiers
            downname = downcase_identifier(curname, len, false, false);
            part = cstring_to_text_with_len(downname, len);
            astate = accumArrayResult(astate, PointerGetDatum(part), false,
                                    TEXTOID, CurrentMemoryContext);
            missing_ident = false;
        }

        if (missing_ident) {
            // Generate appropriate error message
            if (*nextp == '.')
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("string is not a valid identifier: \"%s\"",
                               text_to_cstring(qualname)),
                        errdetail("No valid identifier before \".\".")));
            else if (after_dot)
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("string is not a valid identifier: \"%s\"",
                               text_to_cstring(qualname)),
                        errdetail("No valid identifier after \".\".")));
            else
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("string is not a valid identifier: \"%s\"",
                               text_to_cstring(qualname))));
        }

        // Skip whitespace and handle dots or end of string
        while (scanner_isspace(*nextp))
            nextp++;

        if (*nextp == '.') {
            after_dot = true;
            nextp++;
            while (scanner_isspace(*nextp))
                nextp++;
        } else if (*nextp == '\0') {
            break;
        } else {
            // Invalid character found
            if (strict)
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("string is not a valid identifier: \"%s\"",
                               text_to_cstring(qualname))));
            break;
        }
    }

    PG_RETURN_DATUM(makeArrayResult(astate, CurrentMemoryContext));
}
```