# defGetBoolean

## Location
src/backend/commands/define.c: 107 - 161

## Overview
Extracts a boolean value from a DefElem, supporting various representations including integers (0/1), strings ("true"/"false", "on"/"off"), and no parameter (defaults to true).

## Definition
```c
bool defGetBoolean(DefElem *def)
```

## Detailed Description
The `defGetBoolean` function extracts boolean values from DefElem nodes with flexible input handling. Unlike other defGet functions, it has special default behavior: if no parameter value is given (def->arg == NULL), it assumes "true" is meant. This is useful for SQL syntax where boolean options can be specified without explicit values.

The function accepts multiple representations of boolean values:
- Integer values: 0 (false) and 1 (true)  
- String values: "true", "false", "on", "off" (case-insensitive)
- No parameter: defaults to true

For non-integer types, the function delegates to `defGetString` to convert the value to a string, then performs case-insensitive string comparisons to determine the boolean result.

## Parameters / Member Variables
- `def`: A pointer to a DefElem structure containing the definition element from which to extract a boolean value

## Dependencies
- Functions called/Symbols referenced:
  - DefElem (structure type)
  - nodeTag (for type checking)
  - intVal (to extract integer values)
  - defGetString (to convert non-integer types to strings)
  - pg_strcasecmp (for case-insensitive string comparison)
  - ereport (for error reporting)
  - errcode/errmsg (for error handling)
  
- Called from (representative examples):
  - transformRelOptions (src/backend/access/common/reloptions.c:1306)
  - parse_basebackup_options (src/backend/backup/basebackup.c:743)
  - DefineAggregate (src/backend/commands/aggregatecmds.c:149)
  - cluster (src/backend/commands/cluster.c:124)
  - DefineCollation (src/backend/commands/collationcmds.c:204)
  - ProcessCopyOptions (src/backend/commands/copy.c:510)
  - createdb (src/backend/commands/dbcommands.c:935)
  - ExplainQuery (src/backend/commands/explain.c:201)
  - ExecVacuum (src/backend/commands/vacuum.c:189)

## Notes and Other Information
- Unique among defGet functions in having a default value (true) when no argument is provided
- Accepts multiple string representations of boolean values, matching PostgreSQL's opt_boolean_or_string grammar production
- Performs case-insensitive string matching for string-based boolean values
- Only accepts integer values 0 and 1; other integers cause an error
- The function is located in src/backend/commands/define.c:107-161
- Widely used across PostgreSQL's DDL commands for processing boolean options
- String comparisons use pg_strcasecmp for consistent case-insensitive matching