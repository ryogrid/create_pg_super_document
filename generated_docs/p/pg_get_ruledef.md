# pg_get_ruledef

## Location
[src/backend/utils/adt/ruleutils.c:538-555](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L538-L555)

## Overview
PostgreSQL SQL function that returns a text representation of a rewrite rule definition that could be used as a statement to recreate the rule.

## Definition

```c
Datum
pg_get_ruledef(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as a public interface for retrieving the definition of a PostgreSQL rewrite rule in SQL text format. It takes a rule OID as input and returns the complete rule definition as text that can be executed to recreate the rule. The function uses default pretty-printing with indentation to format the output for better readability.

The function acts as a wrapper around , setting default formatting flags and handling the conversion from C string to PostgreSQL text type.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: OID of the rewrite rule to retrieve the definition for (obtained via )
## Dependencies
- Functions called/Symbols referenced:
  -  - Core worker function that generates the rule definition
  -  - Converts C string to PostgreSQL text type
  -  - Constant for formatting with indentation
  -  - Macro for returning text result
- Called from (representative examples):
  - No direct callers found in the analyzed codebase (likely called via SQL function interface)

## Notes and Other Information
- Located at src/backend/utils/adt/ruleutils.c:538-555
- Returns NULL if the rule definition cannot be retrieved
- Uses fixed pretty-printing flags with indentation enabled
- Part of PostgreSQL's system information functions accessible via SQL