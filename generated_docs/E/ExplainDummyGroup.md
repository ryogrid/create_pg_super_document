# ExplainDummyGroup

## Location
[src/backend/commands/explain.c:5077-5122](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/explain.c#L5077-L5122)

## Overview
ExplainDummyGroup emits a group object that never has any members, used for representing empty or placeholder groups in EXPLAIN output across different formats.

## Definition

```c
structure is an array of plans */
			appendStringInfoChar(es->str, '[');
```
## Detailed Description
ExplainDummyGroup is designed to emit empty group representations in various output formats when a group concept exists but contains no actual members. This is useful for maintaining consistent structure in EXPLAIN output even when certain sections are empty.

The function handles format-specific empty group representation:
- **TEXT format**: No output required (plain text doesn't need explicit empty group notation)
- **XML format**: Emits a self-closing XML tag (e.g., )
- **JSON format**: Emits a simple string value with proper JSON escaping and optional labeling
- **YAML format**: Emits a properly formatted YAML entry, either as a labeled value or as a list item with dash notation

The function ensures that empty groups are represented consistently across all supported EXPLAIN output formats while maintaining proper formatting and escaping rules.

## Parameters / Member Variables
- : The type of the group object (used as the content/tag name in the output)
- : The label name within a containing object (optional - can be NULL for unlabeled groups)
- : ExplainState structure containing formatting information and output buffer

## Dependencies
- Functions called/Symbols referenced:
  - [ExplainXMLTag](ExplainXMLTag.md) (for XML self-closing tag generation)
  - [ExplainJSONLineEnding](ExplainJSONLineEnding.md) (for proper JSON line formatting)
  - [ExplainYAMLLineStarting](ExplainYAMLLineStarting.md) (for proper YAML line formatting)
  - [appendStringInfoSpaces](../a/appendStringInfoSpaces.md) (for indentation)
  - [appendStringInfoString](../a/appendStringInfoString.md) (for string concatenation)
  - [escape_json](../e/escape_json.md) (for JSON string escaping)
  - [escape_yaml](../e/escape_yaml.md) (for YAML string escaping)
- Called from (representative examples):
  - [ExplainOneUtility](ExplainOneUtility.md) (multiple calls for different empty utility command scenarios)

## Notes and Other Information
- This is a static function, only accessible within the explain.c file
- Used when a logical group exists in the EXPLAIN structure but contains no actual data to display
- Maintains format consistency by providing appropriate empty representations rather than omitting the group entirely
- Commonly used in utility command explanations where certain sections may be empty
- Handles proper escaping for string content in JSON and YAML formats to prevent output corruption
- Located in src/backend/commands/explain.c:5070-5114

## Simplified Source

```c
static void
ExplainDummyGroup(const char *objtype, const char *labelname, ExplainState *es)
{
    switch (es->format)
    {
        case EXPLAIN_FORMAT_TEXT:
            // No output needed for text format
            break;

        case EXPLAIN_FORMAT_XML:
            // Emit self-closing XML tag
            ExplainXMLTag(objtype, X_CLOSE_IMMEDIATE, es);
            break;

        case EXPLAIN_FORMAT_JSON:
            // Emit JSON string with proper formatting
            ExplainJSONLineEnding(es);
            appendStringInfoSpaces(es->str, 2 * es->indent);
            if (labelname) {
                escape_json(es->str, labelname);
                appendStringInfoString(es->str, ": ");
            }
            escape_json(es->str, objtype);
            break;

        case EXPLAIN_FORMAT_YAML:
            // Emit YAML entry with proper formatting
            ExplainYAMLLineStarting(es);
            if (labelname) {
                escape_yaml(es->str, labelname);
                appendStringInfoString(es->str, ": ");
            } else {
                appendStringInfoString(es->str, "- ");
            }
            escape_yaml(es->str, objtype);
            break;
    }
}
```