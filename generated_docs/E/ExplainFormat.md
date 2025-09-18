# ExplainFormat

## Location
src/include/commands/explain.h: 33 - 34

## Overview
ExplainFormat is an enumeration that specifies the output format for EXPLAIN command results, controlling how query execution plans are presented to users.

## Definition
typedef enum ExplainFormat
{
    EXPLAIN_FORMAT_TEXT,
    EXPLAIN_FORMAT_XML,
    EXPLAIN_FORMAT_JSON,
    EXPLAIN_FORMAT_YAML,
} ExplainFormat;

## Detailed Description
This enumeration defines the available output formats for the EXPLAIN command in PostgreSQL. It determines how query execution plans, statistics, and analysis information are formatted and presented to the user. The format affects the structure, syntax, and presentation style of the explain output.

Each format serves different use cases: TEXT for human readability, XML/JSON/YAML for programmatic processing and integration with external tools. The format choice influences how various explain functions format their output throughout the explain.c implementation.

## Parameters / Member Variables
- EXPLAIN_FORMAT_TEXT: Traditional plain text format with indentation and human-readable layout
- EXPLAIN_FORMAT_XML: Structured XML format suitable for parsing and automated processing  
- EXPLAIN_FORMAT_JSON: JSON format for easy integration with web applications and APIs
- EXPLAIN_FORMAT_YAML: YAML format providing human-readable structured data representation

## Dependencies
- Functions called/Symbols referenced:
  - Used within ExplainState struct (line 59 in explain.h)
  - Referenced extensively throughout explain.c formatting functions
- Called from (representative examples):
  - [ExplainQuery](ExplainQuery.md) parsing logic (lines 256-262 in explain.c)
  - [ExplainBeginOutput](ExplainBeginOutput.md), ExplainEndOutput formatting functions
  - [ExplainOpenGroup](ExplainOpenGroup.md), ExplainCloseGroup grouping functions
  - Multiple ExplainProperty* functions for value formatting

## Notes and Other Information
- TEXT format is the default and most commonly used format
- Non-TEXT formats use structured grouping with ExplainOpenGroup/ExplainCloseGroup calls
- Format selection affects indentation, labeling, and data organization throughout the explain output
- XML, JSON, and YAML formats enable machine parsing and integration with external analysis tools
- The format is typically specified via the FORMAT option in EXPLAIN statements
- Extensive conditional logic in explain.c switches behavior based on the selected format