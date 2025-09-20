# add_string_reloption

## Location
[src/backend/access/common/reloptions.c:1098-1117](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/reloptions.c#L1098-L1117)

## Overview
Adds a new string-type reloption (relation option) to the PostgreSQL system, allowing tables and other database relations to accept custom string configuration parameters.

## Definition

```c
void
add_string_reloption(bits32 kinds, const char *name, const char *desc,
					 const char *default_val, validate_string_relopt validator,
					 LOCKMODE lockmode)
```
## Detailed Description
This function registers a new string-type reloption in PostgreSQL's reloption system. Reloptions are configuration parameters that can be specified when creating or altering database relations (tables, indexes, etc.). This particular function handles string-valued options and provides optional validation through a callback function. The function internally uses  to create the option structure and  to register it with the system.

## Parameters / Member Variables
- : Bitmask specifying which relation kinds (tables, indexes, etc.) this option applies to
- : The name of the reloption as it will appear in SQL statements
- : Human-readable description of what this option does
- : Default value for the option if not specified by users
- : Optional function pointer for validating option values (can be NULL)
- : Lock mode required when setting this option

## Dependencies
- Functions called/Symbols referenced:
  - [init_string_reloption](../i/init_string_reloption.md)
  - [add_reloption](add_reloption.md)
  - [relopt_string](../r/relopt_string.md)
  - [relopt_gen](../r/relopt_gen.md)
- Called from (representative examples):
  - GET_STRING_RELOPTION (macro)
  - [create_reloptions_table](../c/create_reloptions_table.md) (test module)

## Notes and Other Information
- The validator function, if provided, must call elog(ERROR) when the string value is invalid
- The default value must pass validation if a validator is specified
- This is part of PostgreSQL's extensible reloption system that allows plugins and extensions to define custom relation parameters
- The function is defined in src/backend/access/common/reloptions.c:1098-1117