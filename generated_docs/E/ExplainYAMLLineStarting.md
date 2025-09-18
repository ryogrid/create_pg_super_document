# ExplainYAMLLineStarting

## Location
src/backend/commands/explain.c: 5274 - 5298

## Overview
A utility function that handles proper indentation for YAML line formatting in EXPLAIN output, managing line breaks and spacing according to YAML structure requirements.

## Definition
static void ExplainYAMLLineStarting(ExplainState *es)

## Detailed Description
This function manages the indentation and line formatting for YAML output in PostgreSQL's EXPLAIN command. YAML format requires specific indentation rules with two spaces per indentation level. The function handles two scenarios: for the first property in an unlabeled group, it simply marks the group as started without adding a newline (allowing the property to appear on the same line as the opening dash), while for subsequent properties, it adds a newline followed by appropriate indentation spacing based on the current nesting level.

## Parameters / Member Variables
- es: ExplainState pointer containing the output buffer, current indentation level, and grouping stack for tracking YAML structure nesting

## Dependencies
- Functions called/Symbols referenced:
  - linitial_int (list manipulation function)
  - appendStringInfoChar (string buffer utility)
  - appendStringInfoSpaces (string buffer spacing utility)
  - ExplainState (struct type)
  - EXPLAIN_FORMAT_YAML (format constant)
- Called from (representative examples):
  - [ExplainPropertyList](ExplainPropertyList.md)
  - [ExplainPropertyListNested](ExplainPropertyListNested.md)
  - [ExplainProperty](ExplainProperty.md)
  - [ExplainOpenGroup](ExplainOpenGroup.md)
  - [ExplainDummyGroup](ExplainDummyGroup.md)

## Notes and Other Information
- This is a static function only accessible within the explain.c file
- The function includes an assertion to ensure it's only called when format is YAML
- Uses a two-space indentation standard per YAML specification
- The grouping_stack tracks whether this is the first property in a group to determine line break behavior
- Part of PostgreSQL's EXPLAIN output formatting system for structured data formats