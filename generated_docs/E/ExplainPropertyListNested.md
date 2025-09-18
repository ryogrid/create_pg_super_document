# ExplainPropertyListNested

## Location
src/backend/commands/explain.c: 4696 - 4748

## Overview
Formats and outputs a property that represents a nested list (a list of unlabeled items within another list) with specialized handling for JSON and YAML formats while delegating to ExplainPropertyList for TEXT and XML formats.

## Definition


## Detailed Description
This function provides specialized formatting for nested list structures in EXPLAIN output. It handles the unique requirements of different output formats for nested data:

- **TEXT/XML formats**: Delegates to ExplainPropertyList since these formats handle nested lists the same way as regular lists
- **JSON format**: Creates a JSON array without a property label, suitable for embedding within larger JSON structures
- **YAML format**: Creates a YAML list item using the dash-bracket notation ("- [item1, item2]") which represents an array item within a larger list

This function is specifically designed for cases where list data needs to be represented as an element within another list structure, rather than as a standalone property with its own label.

## Parameters / Member Variables
- : The label/name for the nested property (used only for TEXT/XML delegation)
- : A PostgreSQL List containing C strings to be formatted as a nested list
- : Pointer to ExplainState containing output format information and string buffer

## Dependencies
- Functions called/Symbols referenced:
  - ExplainState (struct type)
  - EXPLAIN_FORMAT_TEXT, EXPLAIN_FORMAT_XML, EXPLAIN_FORMAT_JSON, EXPLAIN_FORMAT_YAML (enum values)
  - ExplainPropertyList
  - ExplainJSONLineEnding
  - ExplainYAMLLineStarting
  - appendStringInfoSpaces, appendStringInfoString, appendStringInfoChar
  - escape_json, escape_yaml
  - lfirst (PostgreSQL list macro)
- Called from (representative examples):
  - show_grouping_set_keys (at src/backend/commands/explain.c:2724)

## Notes and Other Information
- This is a public function (not static), available throughout the PostgreSQL codebase
- The function provides format-specific handling to ensure proper nesting representation
- For TEXT and XML formats, it simply delegates to ExplainPropertyList since these formats don't require special nested handling
- JSON format creates an unlabeled array, making it suitable for inclusion in larger JSON structures
- YAML format uses the "- [...]" notation to represent an array as a list item
- The function maintains proper comma separation and escaping for each supported format
- Primarily used for complex nested structures like grouping set keys where lists need to be embedded within other list structures