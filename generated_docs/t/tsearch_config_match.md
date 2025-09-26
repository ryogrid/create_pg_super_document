# tsearch_config_match

## Location
[src/bin/initdb/initdb.c:860-932](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/initdb/initdb.c#L860-L932)

## Overview
A lookup table structure used by initdb to map language names derived from LC_CTYPE locale settings to corresponding PostgreSQL text search configuration names during database initialization.

## Definition

```c
struct tsearch_config_match
{
	const char *tsconfname;
	const char *langname;
};
```
## Detailed Description
The  structure serves as a mapping table entry that associates language identifiers with PostgreSQL text search configuration names. During database initialization, initdb uses this structure to automatically select an appropriate default text search configuration based on the system's LC_CTYPE locale setting.

The structure is used to populate a static array  that contains mappings for dozens of languages, allowing initdb to intelligently choose text search configurations like "english", "german", "french", etc. based on locale information. This automation helps ensure that newly created databases have sensible defaults for full-text search functionality without requiring manual configuration.

The matching process involves extracting the language portion from LC_CTYPE (stripping country codes and encoding information), then searching through the array to find a corresponding text search configuration name.

## Parameters / Member Variables
- `*tsconfname`: The name of a PostgreSQL text search configuration (e.g., "english", "german", "spanish") that corresponds to a language
- `*langname`: A language identifier string that can be extracted from LC_CTYPE locale settings (e.g., "en", "de", "es", "English", "German")
## Dependencies
- Functions called/Symbols referenced:
  - (Used as a data structure; no function calls from within the struct)
- Called from (representative examples):
  -  (searches through the  array to find matching configurations)
  - Static array initialization  (provides the lookup data)

## Notes and Other Information
- The structure is used exclusively within initdb for automatic text search configuration selection
- The static array  contains 50+ mappings covering major world languages
- Both language codes ("en", "de") and full language names ("English", "German") are supported
- Special handling is provided for POSIX/C locales which default to English text search configuration
- The matching algorithm is case-insensitive and handles various locale string formats
- This automation significantly improves the out-of-box experience for non-English PostgreSQL installations
- The structure supports a NULL terminator pattern in the array (both fields set to NULL) to mark the end of the lookup table