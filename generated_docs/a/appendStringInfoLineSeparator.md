# appendStringInfoLineSeparator

## Location
[src/backend/utils/adt/xml.c:2324-2335](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L2324-L2335)

## Overview
A static utility function that ensures clean line separation in StringInfo buffers by removing any trailing newlines and appending exactly one newline character if the string is non-empty.

## Definition

```c
static void
appendStringInfoLineSeparator(StringInfo str)
```
## Detailed Description
This function provides controlled line separation functionality by first cleaning up any existing trailing newlines using chopStringInfoNewlines(), then conditionally appending a single newline character. The function ensures that non-empty strings end with exactly one newline, which is useful for maintaining consistent formatting in multi-line text output, particularly in XML error handling contexts.

The two-step process (cleanup then append) prevents accumulation of multiple trailing newlines while ensuring proper line separation between text segments.

## Parameters / Member Variables
- : A StringInfo pointer to the string buffer that will be processed. The string is modified in-place to have exactly zero (if empty) or one (if non-empty) trailing newline.

## Dependencies
- Functions called/Symbols referenced:
  - [chopStringInfoNewlines](../c/chopStringInfoNewlines.md) (removes existing trailing newlines)
  - [appendStringInfoChar](appendStringInfoChar.md) (adds the new newline character)
- Called from (representative examples):
  - [xml_errorHandler](../x/xml_errorHandler.md) (multiple call sites for error message formatting)
  - [PgXmlErrorContext](../P/PgXmlErrorContext.md) (for error context formatting)

## Notes and Other Information
- This is a static function, accessible only within the xml.c compilation unit
- The function provides idempotent behavior - calling it multiple times on the same string will always result in exactly one trailing newline
- Empty strings remain empty (no newline is added), which prevents unnecessary formatting of null content
- Primarily used in XML error reporting infrastructure to maintain clean, readable error message formatting
- The conditional append (only if len > 0) prevents adding newlines to empty error messages or contexts

## Simplified Source

```c
static void appendStringInfoLineSeparator(StringInfo str) {
    // Remove any existing trailing newlines
    chopStringInfoNewlines(str);

    // Add exactly one newline if string is not empty
    if (str->len > 0)
        appendStringInfoChar(str, '\n');
}
```