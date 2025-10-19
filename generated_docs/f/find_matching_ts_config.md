# find_matching_ts_config

## Location
[src/bin/initdb/initdb.c:933-978](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/initdb/initdb.c#L933-L978)

## Overview
This function finds a text search configuration that matches the given locale type by extracting the language name and looking it up in a predefined table of supported text search configurations.

## Definition

```c
struct stat statbuf;
```
## Detailed Description
The  function determines an appropriate text search configuration based on the locale type (lc_ctype). It extracts the language portion from the locale string by stripping everything after underscore, hyphen, dot, or @ characters, then searches through the  array to find a matching language name. This is used during database initialization to set up appropriate text search functionality based on the database's locale settings. The function handles various locale formats including Unix-style locales and Windows locale names.

## Parameters / Member Variables
- : The locale type string from which to extract the language name (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [pg_strdup](../p/pg_strdup.md): PostgreSQL utility for string duplication
  - [pg_strcasecmp](../p/pg_strcasecmp.md): PostgreSQL case-insensitive string comparison function
  - free: Standard C library function for memory deallocation
- Global variables accessed:
  - tsearch_config_languages: Array containing language to text search configuration mappings
- Called from (representative examples):
  - [setup_text_search](../s/setup_text_search.md): Used twice during text search configuration setup

## Notes and Other Information
- Returns the configuration name string if a match is found, NULL otherwise
- Handles NULL input by creating an empty language name
- Supports multiple locale formats: Unix locales (language_COUNTRY), Windows locale names (with hyphens), and others
- The parsing stops at multiple delimiters: '_', '-', '.', or '@' to handle various locale naming conventions
- Uses case-insensitive comparison when matching language names
- The returned string is a pointer to a static array element and should not be freed
- Comments indicate potential future enhancement to handle space characters and Norwegian locale variants
- Essential for automatic text search configuration during database initialization based on system locale

## Simplified Source

```c
static const char *
find_matching_ts_config(const char *lc_type)
{
    char *langname, *ptr;

    // Extract language name from locale string
    if (lc_type == NULL)
        langname = pg_strdup("");
    else
    {
        ptr = langname = pg_strdup(lc_type);
        // Stop at locale delimiters: underscore, hyphen, dot, or @
        while (*ptr &&
               *ptr != '_' && *ptr != '-' && *ptr != '.' && *ptr != '@')
            ptr++;
        *ptr = '\0';
    }

    // Search for matching text search configuration
    for (int i = 0; tsearch_config_languages[i].tsconfname; i++)
    {
        if (pg_strcasecmp(tsearch_config_languages[i].langname, langname) == 0)
        {
            free(langname);
            return tsearch_config_languages[i].tsconfname;
        }
    }

    free(langname);
    return NULL;
}
```