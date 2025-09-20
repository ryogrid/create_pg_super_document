# locate_stem_module

## Location
[src/backend/snowball/dict_snowball.c:179-219](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/dict_snowball.c#L179-L219)

## Overview
This function locates and initializes the appropriate Snowball stemmer module for a given language and encoding combination.

## Definition

```c
static void
locate_stem_module(DictSnowball *d, const char *lang)
```
## Detailed Description
The function searches for a compatible Snowball stemmer module through a two-phase approach. First, it attempts to find an exact match for the specified language that works with the current database encoding (ASCII stemmers are considered compatible with any encoding). If no exact match is found, it falls back to searching for a UTF-8 version of the stemmer and sets up encoding conversion if necessary. The function throws an error if no suitable stemmer is available for the language-encoding combination.

## Parameters / Member Variables
- : Pointer to DictSnowball structure that will be configured with the found stemmer
- : String specifying the target language for stemming (e.g., "english", "spanish")

## Dependencies
- Functions called/Symbols referenced:
  - [GetDatabaseEncoding](../G/GetDatabaseEncoding.md)
  - [GetDatabaseEncodingName](../G/GetDatabaseEncodingName.md)
  - [pg_strcasecmp](../p/pg_strcasecmp.md)
  - ereport
  - stemmer_modules (global array)
  - PG_SQL_ASCII (encoding constant)
  - PG_UTF8 (encoding constant)
- Called from (representative examples):
  - [dsnowball_init](../d/dsnowball_init.md)

## Notes and Other Information
- This is a static function internal to the Snowball dictionary implementation
- The function prioritizes exact encoding matches over UTF-8 fallbacks to avoid unnecessary encoding conversions
- ASCII stemmers are treated as encoding-agnostic and compatible with any database encoding
- If a UTF-8 stemmer is used with a non-UTF-8 database encoding, the needrecode flag is set to enable automatic encoding conversion during stemming operations