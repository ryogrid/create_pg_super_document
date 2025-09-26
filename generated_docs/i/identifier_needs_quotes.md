# identifier_needs_quotes

## Location
[src/bin/psql/tab-complete.c:6180-6212](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/tab-complete.c#L6180-L6212)

## Overview
Detects whether a SQL identifier must be double-quoted by checking syntax rules and keyword conflicts in psql's tab completion system.

## Definition
```c
static bool
identifier_needs_quotes(const char *ident)
```

## Detailed Description
The `identifier_needs_quotes` function determines whether a SQL identifier requires double-quoting to be valid. It performs comprehensive validation by checking both syntactic requirements and keyword conflicts. The function follows PostgreSQL's identifier quoting rules:

1. **Syntax Check**: Verifies the identifier starts with a lowercase letter or underscore and contains only valid characters (lowercase letters, digits, underscores, and dollar signs)
2. **Character Set Validation**: Ensures all characters are ASCII and within the allowed set
3. **Keyword Conflict Check**: Uses ScanKeywordLookup to identify if the identifier conflicts with SQL keywords, requiring quoting for all keywords except unreserved ones

The function is conservative, requiring quotes for any non-ASCII characters, matching the behavior of the backend's quote_ident() function.

## Parameters / Member Variables
- `ident`: The identifier string to be checked for quoting requirements

## Dependencies
- Functions called/Symbols referenced:
  - [ScanKeywordLookup](../S/ScanKeywordLookup.md) (for keyword validation against ScanKeywords)
  - UNRESERVED_KEYWORD (constant for keyword category comparison)
  - ScanKeywordCategories (array for keyword category lookup)
- Called from (representative examples):
  - THING_NO_SHOW (completion handling)
  - [_complete_from_query](../c/_complete_from_query.md) (multiple calls for query-based completion)
  - [requote_identifier](../r/requote_identifier.md) (called twice for schema and object name validation)

## Notes and Other Information
- Returns true if the identifier requires double-quoting, false otherwise
- Part of psql's tab completion system in PostgreSQL
- Located in src/bin/psql/tab-complete.c at lines 6180-6212
- The function is static, meaning it's only accessible within the tab-complete.c file
- Uses case-insensitive keyword comparison but assumes input is already lowercase
- Conservative approach: quotes anything that's not ASCII, similar to backend behavior
- The keyword list used may not exactly match the server's but is sufficient for tab-completion purposes