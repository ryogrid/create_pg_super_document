# PQfnumber

## Location
[src/interfaces/libpq/fe-exec.c:3589-3685](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L3589-L3685)

## Overview
A public libpq function that finds the field (column) number for a given column name in a query result set, implementing SQL-style identifier parsing and case handling.

## Definition
```c
int PQfnumber(const PGresult *res, const char *field_name)
```

## Detailed Description
PQfnumber performs a reverse lookup to find the 0-based column number corresponding to a given column name. The function implements SQL identifier parsing rules, including case-folding (converting unquoted identifiers to lowercase) and double-quote processing for quoted identifiers. It includes an optimization path for all-lowercase field names that avoids string duplication and parsing overhead. The function handles both quoted and unquoted identifiers according to SQL standards, making it suitable for applications that need to map user-specified column names to their numeric indices.

## Parameters / Member Variables
- `res`: Pointer to a PGresult structure containing the query result data
- `field_name`: The column name to search for, which will be parsed according to SQL identifier rules

## Dependencies
- Functions called/Symbols referenced:
  - [pg_tolower](../p/pg_tolower.md) (for case-folding of unquoted characters)
- Called from (representative examples):
  - No direct references found in the current codebase analysis

## Notes and Other Information
- Returns the 0-based field number if found, -1 if not found or on error
- Returns -1 if res is NULL, field_name is NULL/empty, or res->attDescs is NULL
- Implements SQL identifier parsing: unquoted names are case-folded to lowercase, quoted names preserve case
- Handles escaped quotes within quoted identifiers ("" becomes ")
- Includes performance optimization for all-lowercase names to avoid string duplication
- May find the first match if multiple columns have the same name (though this is rare)
- Part of the public libpq API (declared in libpq-fe.h)
- Uses dynamic memory allocation (strdup/free) for complex identifier parsing
- Does not fully validate SQL identifier syntax (e.g., partially quoted strings are processed without error)

## Simplified Source

```c
int PQfnumber(const PGresult *res, const char *field_name) {
    char *field_case;
    bool in_quotes;
    bool all_lower = true;
    const char *iptr;
    char *optr;
    int i;

    // Basic validation
    if (!res || !field_name || field_name[0] == '\0' || res->attDescs == NULL)
        return -1;

    // Fast path: check if field name is already all lowercase
    for (iptr = field_name; *iptr; iptr++) {
        char c = *iptr;
        if (c == '"' || c != pg_tolower((unsigned char) c)) {
            all_lower = false;
            break;
        }
    }

    // If all lowercase, do direct comparison
    if (all_lower) {
        for (i = 0; i < res->numAttributes; i++)
            if (strcmp(field_name, res->attDescs[i].name) == 0)
                return i;
    }

    // Complex case: handle quoted identifiers and case-folding
    field_case = strdup(field_name);
    if (field_case == NULL)
        return -1;

    // Parse SQL identifier: handle quotes and case-folding
    in_quotes = false;
    optr = field_case;
    for (iptr = field_case; *iptr; iptr++) {
        char c = *iptr;

        if (in_quotes) {
            if (c == '"') {
                if (iptr[1] == '"') {
                    // Double quote becomes single quote
                    *optr++ = '"';
                    iptr++;
                } else {
                    in_quotes = false;
                }
            } else {
                *optr++ = c;
            }
        } else if (c == '"') {
            in_quotes = true;
        } else {
            // Convert unquoted characters to lowercase
            *optr++ = pg_tolower((unsigned char) c);
        }
    }
    *optr = '\0';

    // Search for processed field name
    for (i = 0; i < res->numAttributes; i++) {
        if (strcmp(field_case, res->attDescs[i].name) == 0) {
            free(field_case);
            return i;
        }
    }

    free(field_case);
    return -1;
}
```