# tsvector_update_trigger_byid

## Location
[src/backend/utils/adt/tsvector_op.c:2727-2732](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsvector_op.c#L2727-L2732)

## Overview
A PostgreSQL trigger function that automatically updates a tsvector column based on text column(s), using configuration specified by column ID rather than column name.

## Definition

```c
Datum
tsvector_update_trigger_byid(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a database trigger function designed to automatically maintain tsvector columns when related text columns are modified. This function is a wrapper that calls the main  implementation with the  parameter, indicating that the text search configuration should be identified by a regconfig column ID rather than by name.

This trigger function is typically used in scenarios where the text search configuration is stored as a regconfig value in a column of the same table, allowing for row-specific or dynamically determined text search configurations. This provides more flexibility compared to using a fixed configuration name.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing trigger-specific arguments:
  - Argument 0: Name of the tsvector column to update
  - Argument 1: Name of the regconfig column containing the configuration ID
  - Remaining arguments: Names of text columns to be processed into the tsvector

## Dependencies
- Functions called/Symbols referenced:
  -  - Main trigger implementation function (called with )
- Called from (representative examples):
  - No direct references found (likely called via PostgreSQL trigger system)

## Notes and Other Information
- This function is specifically designed to be used as a PostgreSQL trigger function
- The second argument must be a regconfig column name rather than a literal configuration name
- Part of PostgreSQL's automatic tsvector maintenance system for full-text search
- The  parameter passed to the main function indicates that configuration lookup should be by column ID
- [Trigger](../T/Trigger.md) arguments follow a specific pattern: tsvector_column, regconfig_column, text_column1, [text_column2, ...]
- The regconfig column should contain valid text search configuration OIDs
- Automatically called when INSERT or UPDATE operations occur on the table where this trigger is installed