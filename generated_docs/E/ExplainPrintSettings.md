# ExplainPrintSettings

## Location
src/backend/commands/explain.c: 807 - 876

## Overview
ExplainPrintSettings prints a summary of modified GUC (Grand Unified Configuration) settings that affect query planning in EXPLAIN output.

## Definition


## Detailed Description
ExplainPrintSettings generates output showing configuration settings that have been modified from their defaults and could influence query planning decisions. This information helps users understand why the planner made certain choices by showing relevant non-default configuration values.

The function retrieves a list of explain-relevant GUC options using get_explain_guc_options() and formats them according to the output format:

- For structured formats (JSON, XML, YAML): Each setting is output as a separate property within a "Settings" group
- For TEXT format: All settings are combined into a single comma-separated line, or omitted entirely if no relevant settings are modified

The function only outputs information when the SETTINGS option is enabled in the ExplainState, allowing users to control whether configuration details are included in the output.

## Parameters / Member Variables
- : ExplainState containing output formatting options and the settings flag that controls whether configuration information should be included

## Dependencies
- Functions called/Symbols referenced:
  - get_explain_guc_options
  - [GetConfigOptionByName](../G/GetConfigOptionByName.md)
  - [ExplainOpenGroup](ExplainOpenGroup.md)
  - [ExplainCloseGroup](ExplainCloseGroup.md)
  - [ExplainPropertyText](ExplainPropertyText.md)
  - initStringInfo
  - appendStringInfoString
  - appendStringInfo
- Called from (representative examples):
  - [ExplainPrintPlan](ExplainPrintPlan.md)

## Notes and Other Information
- The function is static (internal to explain.c) and only called from ExplainPrintPlan
- In TEXT format, if no relevant settings are modified, no output is generated at all
- The function handles NULL setting values gracefully by displaying "= NULL"
- Settings are retrieved by name and formatted consistently regardless of their data type
- The selection of which GUCs are "explain-relevant" is determined by get_explain_guc_options()
- This feature helps with query performance analysis by showing configuration context that affects planning decisions