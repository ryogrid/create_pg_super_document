# JsonLikeRegexContext

## Location
src/backend/utils/adt/jsonpath_exec.c: 122 - 126

## Overview
A context structure that holds regular expression pattern and compilation flags for executing LIKE_REGEX operations in JSON path expressions.

## Definition
```c
typedef struct JsonLikeRegexContext
{
    text           *regex;
    int             cflags;
} JsonLikeRegexContext;
```

## Detailed Description
JsonLikeRegexContext is a simple structure used to encapsulate the state needed for regular expression matching in JSON path LIKE_REGEX operations. It stores the compiled regular expression pattern as text and the associated compilation flags that control regex behavior such as case sensitivity, multi-line matching, and other regex options.

## Parameters / Member Variables
- `regex`: Pointer to a text structure containing the regular expression pattern string
- `cflags`: Integer containing compilation flags that control regex matching behavior (such as case sensitivity, multi-line mode, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - [text](../t/text.md) (PostgreSQL text type)
- Called from (representative examples):
  - [executeBoolItem](../e/executeBoolItem.md)
  - [executeLikeRegex](../e/executeLikeRegex.md)

## Notes and Other Information
- This structure is specifically designed for the LIKE_REGEX JSON path operator
- The cflags field typically contains standard POSIX regex compilation flags
- Used internally during JSON path expression evaluation when regex matching is required
- The structure provides a clean separation between the regex pattern and its compilation options