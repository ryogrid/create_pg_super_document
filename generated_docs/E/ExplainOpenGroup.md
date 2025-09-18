# ExplainOpenGroup

## Location
src/backend/commands/explain.c: 4867 - 4929

## Overview
Opens a group of related objects in PostgreSQL EXPLAIN output, handling the format-specific opening syntax for grouping related properties or objects across different output formats.

## Definition
void ExplainOpenGroup(const char *objtype, const char *labelname, bool labeled, ExplainState *es)

## Detailed Description
This function handles the opening of logical groups in explain output, providing format-specific implementations for TEXT, XML, JSON, and YAML formats. It manages the hierarchical structure of explain output by handling indentation, grouping syntax, and maintaining state through the grouping_stack. For JSON and YAML formats, it maintains a stack-based system to track grouping levels and whether content has been emitted at each level, which is crucial for proper comma placement and formatting. The labeled parameter determines whether group members will be treated as labeled properties or unlabeled array elements.

## Parameters / Member Variables
- `objtype`: The type identifier of the group object being opened (used for XML tags)
- `labelname`: Optional label name for the group within its containing object (NULL if unlabeled)
- `labeled`: Boolean flag indicating whether group members are labeled properties (true) or unlabeled objects (false)
- `es`: Pointer to ExplainState structure containing output format, indentation, and grouping state

## Dependencies
- Functions called/Symbols referenced:
  - EXPLAIN_FORMAT_TEXT, EXPLAIN_FORMAT_XML, EXPLAIN_FORMAT_JSON, EXPLAIN_FORMAT_YAML (format constants)
  - [ExplainXMLTag](ExplainXMLTag.md) (for XML tag generation with X_OPENING parameter)
  - [ExplainJSONLineEnding](ExplainJSONLineEnding.md) (for JSON line ending management)
  - appendStringInfoSpaces (for JSON indentation)
  - [escape_json](../e/escape_json.md) (for JSON string escaping)
  - appendStringInfoString, appendStringInfoChar (for string building)
  - [ExplainYAMLLineStarting](ExplainYAMLLineStarting.md) (for YAML line starting management)
  - [lcons_int](../l/lcons_int.md) (for maintaining integer stacks for grouping state)
- Called from (representative examples):
  - [ExplainOnePlan](ExplainOnePlan.md) (for planning and execution groupings)
  - [ExplainPrintSettings](ExplainPrintSettings.md), ExplainPrintTriggers, ExplainPrintJIT (for various information groups)
  - [ExplainNode](ExplainNode.md) (for node-specific groupings)
  - [show_grouping_sets](../s/show_grouping_sets.md), show_incremental_sort_group_info (for specialized data groupings)
  - [ExplainFlushWorkersState](ExplainFlushWorkersState.md) (for parallel worker information)

## Notes and Other Information
- Critical function for maintaining proper hierarchical structure in explain output
- Each format has distinct requirements: XML uses tags, JSON uses braces/brackets, YAML uses dashes and colons
- The grouping_stack mechanism is essential for proper formatting in JSON and YAML formats
- Manages indentation levels across all formats to ensure proper nesting visualization
- Works in conjunction with ExplainCloseGroup to form complete grouping pairs
- The labeled parameter affects JSON output format (using curly braces {} for labeled, square brackets [] for unlabeled)
- TEXT format implementation is minimal since it relies primarily on indentation for grouping
- Essential for creating nested structures like JIT information, trigger details, and parallel worker data