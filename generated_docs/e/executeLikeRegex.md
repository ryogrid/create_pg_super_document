# executeLikeRegex

## Location
src/backend/utils/adt/jsonpath_exec.c: 2267 - 2297

## Overview
A JSON path predicate callback function that performs regular expression pattern matching on string values.

## Definition
```c
static JsonPathBool executeLikeRegex(JsonPathItem *jsp, JsonbValue *str, JsonbValue *rarg, void *param)
```

## Detailed Description
The `executeLikeRegex` function implements the LIKE_REGEX predicate functionality for JSON path expressions. It checks if a string matches a regular expression pattern by utilizing PostgreSQL's regex engine. The function caches the compiled regex pattern and flags for efficiency when called multiple times with the same pattern. It converts the JSON string value to a scalar and then executes the regex match using the RE_compile_and_execute function with appropriate collation settings.

## Parameters / Member Variables
- `jsp`: JsonPathItem pointer containing the regex pattern and flags
- `str`: JsonbValue pointer representing the string to match against
- `rarg`: JsonbValue pointer for regex argument (currently unused)
- `param`: void pointer to JsonLikeRegexContext for caching compiled regex

## Dependencies
- Functions called/Symbols referenced:
  - getScalar: Converts JsonbValue to scalar string type
  - cstring_to_text_with_len: Converts C string to PostgreSQL text type
  - jspConvertRegexFlags: Converts JSON path regex flags to PostgreSQL regex flags
  - RE_compile_and_execute: Compiles and executes regular expression
  - JsonLikeRegexContext: Context structure for caching regex compilation
  - DEFAULT_COLLATION_OID: Default collation for regex matching
- Called from (representative examples):
  - executeBoolItem: Main boolean item execution function
  - RETURN_ERROR: Error handling macro

## Notes and Other Information
- Returns JsonPathBool values: jpbTrue on match, jpbFalse on no match, jpbUnknown on error
- Caches compiled regex pattern and flags in JsonLikeRegexContext to avoid recompilation
- Uses PostgreSQL's regex engine with full Unicode and collation support
- Input string must be convertible to string scalar; non-string values result in jpbUnknown
- Part of PostgreSQL's JSON path expression evaluation system for pattern matching operations