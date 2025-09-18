# defGetBoolean

## Location
[src/backend/commands/define.c:107-161](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/define.c#L107-L161)

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
  - [DefElem](../D/DefElem.md) (structure type)
  - nodeTag (for type checking)
  - intVal (to extract integer values)
  - [defGetString](defGetString.md) (to convert non-integer types to strings)
  - [pg_strcasecmp](../p/pg_strcasecmp.md) (for case-insensitive string comparison)
  - ereport (for error reporting)
  - [errcode](../e/errcode.md)/errmsg (for error handling)
  
- Called from (representative examples):
  - [transformRelOptions](../t/transformRelOptions.md) (src/backend/access/common/reloptions.c:1306)
  - [parse_basebackup_options](../p/parse_basebackup_options.md) (src/backend/backup/basebackup.c:743)
  - [DefineAggregate](../D/DefineAggregate.md) (src/backend/commands/aggregatecmds.c:149)
  - [cluster](../c/cluster.md) (src/backend/commands/cluster.c:124)
  - [DefineCollation](../D/DefineCollation.md) (src/backend/commands/collationcmds.c:204)
  - [ProcessCopyOptions](../P/ProcessCopyOptions.md) (src/backend/commands/copy.c:510)
  - [createdb](../c/createdb.md) (src/backend/commands/dbcommands.c:935)
  - [ExplainQuery](../E/ExplainQuery.md) (src/backend/commands/explain.c:201)
  - ExecVacuum (src/backend/commands/vacuum.c:189)

## Notes and Other Information
- Unique among defGet functions in having a default value (true) when no argument is provided
- Accepts multiple string representations of boolean values, matching PostgreSQL's opt_boolean_or_string grammar production
- Performs case-insensitive string matching for string-based boolean values
- Only accepts integer values 0 and 1; other integers cause an error
- The function is located in src/backend/commands/define.c:107-161
- Widely used across PostgreSQL's DDL commands for processing boolean options
- String comparisons use pg_strcasecmp for consistent case-insensitive matching