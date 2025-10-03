# quoteOneName

## Location
[src/backend/utils/adt/ri_triggers.c:1873-1892](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ri_triggers.c#L1873-L1892)

## Overview
Safely quotes a single SQL identifier name by wrapping it in double quotes and escaping any embedded double quotes.

## Definition
```c
static void quoteOneName(char *buffer, const char *name)
```

## Detailed Description
This is a utility function that ensures SQL identifier names are properly quoted to handle cases where they might contain special characters, spaces, or be reserved words. The function implements a conservative approach by always adding double quotes around the name rather than trying to determine if quoting is necessary.

The function performs the following operations:
1. **Opening Quote**: Adds an opening double quote to the buffer
2. **Character Iteration**: Processes each character in the input name
3. **Quote Escaping**: Doubles any embedded double quotes (SQL standard escaping)
4. **Character Copying**: Copies each character to the output buffer
5. **Closing Quote**: Adds a closing double quote
6. **Null Termination**: Ensures the output string is null-terminated

For example:
- Input:  → Output: 
- Input:  → Output: 
- Input:  → Output: 

## Parameters / Member Variables
- `*buffer`: Output buffer that must be at least MAX_QUOTED_NAME_LEN characters long (includes room for null terminator)
- `*name`: The SQL identifier name to be quoted
## Dependencies
- Functions called/Symbols referenced:
  - None (uses only standard C string operations)
- Called from (representative examples):
  - [ri_Check_Pk_Match](../r/ri_Check_Pk_Match.md)
  - [ri_restrict](../r/ri_restrict.md)
  - [RI_FKey_cascade_del](../R/RI_FKey_cascade_del.md)
  - [RI_FKey_cascade_upd](../R/RI_FKey_cascade_upd.md)
  - [ri_set](../r/ri_set.md)
  - [RI_Initial_Check](../R/RI_Initial_Check.md)
  - [RI_PartitionRemove_Check](../R/RI_PartitionRemove_Check.md)
  - [quoteRelationName](quoteRelationName.md)
  - [ri_GenerateQualCollation](../r/ri_GenerateQualCollation.md)

## Notes and Other Information
- This is a static function local to ri_triggers.c, not exposed in header files
- Uses a conservative "always quote" approach rather than intelligent quoting
- Follows SQL standard for escaping embedded double quotes by doubling them
- Buffer size requirement is MAX_QUOTED_NAME_LEN to accommodate quoted names
- Located in src/backend/utils/adt/ri_triggers.c:1873-1892
- Essential utility for building safe SQL queries in referential integrity operations
- Part of PostgreSQL's SQL injection prevention mechanisms when building dynamic queries

## Simplified Source

```c
static void quoteOneName(char *buffer, const char *name) {
    // Always quote the name for safety - start with opening quote
    *buffer++ = '"';

    // Copy each character, escaping embedded quotes by doubling them
    while (*name) {
        if (*name == '"')
            *buffer++ = '"';  // Double the quote for SQL escaping
        *buffer++ = *name++;
    }

    // Add closing quote and null terminator
    *buffer++ = '"';
    *buffer = '\0';
}
```