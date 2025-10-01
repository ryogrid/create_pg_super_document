# executeLikeRegex

## Location
[src/backend/utils/adt/jsonpath_exec.c:2267-2297](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath_exec.c#L2267-L2297)

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
  - [getScalar](../g/getScalar.md): Converts JsonbValue to scalar string type
  - [cstring_to_text_with_len](../c/cstring_to_text_with_len.md): Converts C string to PostgreSQL text type
  - jspConvertRegexFlags: Converts JSON path regex flags to PostgreSQL regex flags
  - [RE_compile_and_execute](../R/RE_compile_and_execute.md): Compiles and executes regular expression
  - [JsonLikeRegexContext](../J/JsonLikeRegexContext.md): Context structure for caching regex compilation
  - DEFAULT_COLLATION_OID: Default collation for regex matching
- Called from (representative examples):
  - [executeBoolItem](executeBoolItem.md): Main boolean item execution function
  - RETURN_ERROR: Error handling macro

## Notes and Other Information
- Returns JsonPathBool values: jpbTrue on match, jpbFalse on no match, jpbUnknown on error
- Caches compiled regex pattern and flags in JsonLikeRegexContext to avoid recompilation
- Uses PostgreSQL's regex engine with full Unicode and collation support
- Input string must be convertible to string scalar; non-string values result in jpbUnknown
- Part of PostgreSQL's JSON path expression evaluation system for pattern matching operations

## Simplified Source

```c
static JsonPathBool
executeLikeRegex(JsonPathItem *jsp, JsonbValue *str, JsonbValue *rarg, void *param) {
    JsonLikeRegexContext *cxt = param;

    // Ensure input is a string scalar
    if (!(str = getScalar(str, jbvString)))
        return jpbUnknown;

    // Cache regex pattern and flags on first use
    if (!cxt->regex) {
        cxt->regex = cstring_to_text_with_len(jsp->content.like_regex.pattern,
                                             jsp->content.like_regex.patternlen);
        jspConvertRegexFlags(jsp->content.like_regex.flags, &(cxt->cflags), NULL);
    }

    // Execute regex match
    if (RE_compile_and_execute(cxt->regex, str->val.string.val,
                              str->val.string.len, cxt->cflags,
                              DEFAULT_COLLATION_OID, 0, NULL))
        return jpbTrue;

    return jpbFalse;
}
```