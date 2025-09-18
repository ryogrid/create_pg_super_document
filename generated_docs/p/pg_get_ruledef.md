# pg_get_ruledef

## Location
src/backend/utils/adt/ruleutils.c: 538 - 555

## Overview
PostgreSQL SQL function that returns a text representation of a rewrite rule definition that could be used as a statement to recreate the rule.

## Definition


## Detailed Description
This function serves as a public interface for retrieving the definition of a PostgreSQL rewrite rule in SQL text format. It takes a rule OID as input and returns the complete rule definition as text that can be executed to recreate the rule. The function uses default pretty-printing with indentation to format the output for better readability.

The function acts as a wrapper around , setting default formatting flags and handling the conversion from C string to PostgreSQL text type.

## Parameters / Member Variables
- : OID of the rewrite rule to retrieve the definition for (obtained via )

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