# pg_get_viewdef_wrap

## Location
[src/backend/utils/adt/ruleutils.c:695-714](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L695-L714)

## Overview
Provides a PostgreSQL function interface to retrieve the SQL definition of a view with optional line wrapping support.

## Definition


## Detailed Description
This function serves as a PostgreSQL SQL function entry point for retrieving view definitions with line wrapping capabilities. It takes a view OID and a wrap parameter, then delegates to the core worker function  to generate the actual view definition string. The function automatically enables pretty printing flags and returns the result as a PostgreSQL text datum.

## Parameters / Member Variables
- : OID of the view whose definition is to be retrieved
- : Integer parameter controlling line wrapping behavior in the output

## Dependencies
- Functions called/Symbols referenced:
  - GET_PRETTY_FLAGS
  - [pg_get_viewdef_worker](pg_get_viewdef_worker.md)
  - string_to_text
  - PG_RETURN_TEXT_P
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This function is part of PostgreSQL's rule utilities system
- Located in src/backend/utils/adt/ruleutils.c:695-714
- Returns NULL if the view definition cannot be retrieved
- Automatically enables pretty printing for better formatted output
- Part of the PostgreSQL function interface accessible via SQL