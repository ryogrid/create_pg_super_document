# get_publications_str

## Location
[src/backend/commands/subscriptioncmds.c:455-485](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/subscriptioncmds.c#L455-L485)

## Overview
Formats a list of publication names into a comma-separated string with proper quoting for SQL contexts or literal representation.

## Definition

```c
static void
get_publications_str(List *publications, StringInfo dest, bool quote_literal)
```
## Detailed Description
This utility function converts a list of publication names into a properly formatted string representation. It iterates through the provided list of publications and concatenates them with comma separators, applying appropriate quoting based on the quote_literal parameter. When quote_literal is true, it uses PostgreSQL's quote_literal_cstr() function for SQL-safe literal quoting. When false, it simply wraps each publication name in double quotes.

The function is designed to handle the common need to format publication lists for various SQL operations, error messages, and logging contexts within the subscription management system.

## Parameters / Member Variables
- `*publications`: List of publication names (as String nodes) to be formatted
- `dest`: StringInfo buffer where the formatted string will be appended
- `quote_literal`: Boolean flag determining the quoting style - true uses SQL literal quoting, false uses simple double-quote wrapping
## Dependencies
- Functions called/Symbols referenced:
  - strVal: Extracts string value from List node
  - [appendStringInfoString](../a/appendStringInfoString.md): Appends string to StringInfo buffer
  - [appendStringInfoChar](../a/appendStringInfoChar.md): Appends single character to StringInfo buffer
  - [quote_literal_cstr](../q/quote_literal_cstr.md): Applies SQL literal quoting to string
- Called from (representative examples):
  - [check_publications](../c/check_publications.md): For formatting publication lists in error messages and validation
  - [check_publications_origin](../c/check_publications_origin.md): When validating publication origins
  - [fetch_table_list](../f/fetch_table_list.md): During table list retrieval operations

## Notes and Other Information
- The function assumes the publications list is not NIL and will assert if passed an empty list
- The comma-separated format matches PostgreSQL's standard convention for lists in SQL contexts
- The dual quoting modes allow the same function to be used for both SQL query construction and user-facing error messages
- First publication in the list is handled specially to avoid leading comma

## Simplified Source

```c
static void
get_publications_str(List *publications, StringInfo dest, bool quote_literal)
{
    bool first = true;

    // Iterate through each publication name
    foreach(ListCell *lc, publications)
    {
        char *pubname = strVal(lfirst(lc));

        // Add comma separator after first item
        if (first)
            first = false;
        else
            appendStringInfoString(dest, ", ");

        // Apply appropriate quoting style
        if (quote_literal)
            appendStringInfoString(dest, quote_literal_cstr(pubname));
        else {
            appendStringInfoChar(dest, '"');
            appendStringInfoString(dest, pubname);
            appendStringInfoChar(dest, '"');
        }
    }
}
```