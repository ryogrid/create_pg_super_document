# ExplainXMLTag

## Location
src/backend/commands/explain.c: 5212 - 5238

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
  - ExplainState (structure type)
  - X_NOWHITESPACE (flag constant)
  - X_CLOSING (flag constant)
  - X_CLOSE_IMMEDIATE (flag constant)
  - appendStringInfoSpaces (string formatting function)
  - appendStringInfoCharMacro (string formatting macro)
- Called from (representative examples):
  - ExplainPropertyList
  - ExplainProperty
  - ExplainOpenGroup
  - ExplainCloseGroup
  - ExplainDummyGroup

## Notes and Other Information
This is a static function internal to explain.c. The character validation ensures XML compliance by restricting tag names to alphanumeric characters, hyphens, underscores, and periods. Invalid characters are replaced with dashes. The function is essential for generating well-formed XML output in EXPLAIN XML format.