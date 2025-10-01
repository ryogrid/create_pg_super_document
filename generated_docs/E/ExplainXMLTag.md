# ExplainXMLTag

## Location
[src/backend/commands/explain.c:5212-5238](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/explain.c#L5212-L5238)

## Overview
Emits opening or closing XML tags for EXPLAIN output in XML format. This function handles XML tag generation with proper character validation and formatting.

## Definition
static void ExplainXMLTag(const char *tagname, int flags, ExplainState *es)

## Detailed Description
ExplainXMLTag generates properly formatted XML tags for EXPLAIN output. The function sanitizes tag names by replacing invalid XML characters (such as whitespace and slashes) with dashes to ensure XML compliance. It supports different tag types based on flags: opening tags, closing tags, or self-closing tags. The function also handles indentation and whitespace formatting unless suppressed by the X_NOWHITESPACE flag.

## Parameters / Member Variables
- `tagname`: The name of the XML tag to generate
- `flags`: Control flags specifying tag type and formatting (X_OPENING, X_CLOSING, X_CLOSE_IMMEDIATE, X_NOWHITESPACE)
- `es`: ExplainState pointer containing the output string buffer and indentation level

## Dependencies
- Functions called/Symbols referenced:
  - [ExplainState](ExplainState.md) (structure type)
  - X_NOWHITESPACE (flag constant)
  - X_CLOSING (flag constant)
  - X_CLOSE_IMMEDIATE (flag constant)
  - [appendStringInfoSpaces](../a/appendStringInfoSpaces.md) (string formatting function)
  - appendStringInfoCharMacro (string formatting macro)
- Called from (representative examples):
  - [ExplainPropertyList](ExplainPropertyList.md)
  - [ExplainProperty](ExplainProperty.md)
  - [ExplainOpenGroup](ExplainOpenGroup.md)
  - [ExplainCloseGroup](ExplainCloseGroup.md)
  - [ExplainDummyGroup](ExplainDummyGroup.md)

## Notes and Other Information
This is a static function internal to explain.c. The character validation ensures XML compliance by restricting tag names to alphanumeric characters, hyphens, underscores, and periods. Invalid characters are replaced with dashes. The function is essential for generating well-formed XML output in EXPLAIN XML format.

## Simplified Source

```c
static void ExplainXMLTag(const char *tagname, int flags, ExplainState *es)
{
    const char *valid = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.";

    // Add indentation unless suppressed
    if ((flags & X_NOWHITESPACE) == 0)
        appendStringInfoSpaces(es->str, 2 * es->indent);

    // Start tag
    appendStringInfoCharMacro(es->str, '<');
    if ((flags & X_CLOSING) != 0)
        appendStringInfoCharMacro(es->str, '/');

    // Sanitize tag name: replace invalid XML characters with dashes
    for (const char *s = tagname; *s; s++)
        appendStringInfoChar(es->str, strchr(valid, *s) ? *s : '-');

    // Handle self-closing tags
    if ((flags & X_CLOSE_IMMEDIATE) != 0)
        appendStringInfoString(es->str, " /");

    // Close tag and add newline unless suppressed
    appendStringInfoCharMacro(es->str, '>');
    if ((flags & X_NOWHITESPACE) == 0)
        appendStringInfoCharMacro(es->str, '\n');
}
```