# pg_get_indexdef_ext

## Location
[src/backend/utils/adt/ruleutils.c:1178-1204](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L1178-L1204)

## Overview
An extended SQL-callable function that returns either the complete CREATE INDEX statement or a specific column expression for a given index OID with formatting options.

## Definition


## Detailed Description
This function provides an extended interface for retrieving index definitions from PostgreSQL's system catalogs with additional control over output format and content. Unlike the basic , this function accepts a column number parameter that allows retrieving either the complete index definition (when colno=0) or just the expression for a specific index column (when colno>0). It also accepts a pretty-printing flag to control output formatting. The function delegates the actual work to  with parameters derived from the input arguments.

## Parameters / Member Variables
- : The OID of the index to retrieve the definition for
- : Column number - 0 for complete definition, >0 for specific column expression
- : Boolean flag for pretty-printing output format

## Dependencies
- Functions called/Symbols referenced:
  - : Extracts OID argument from function call
  - : Extracts integer argument from function call
  - : Extracts boolean argument from function call
  - : Converts boolean to formatting flags
  - : Core function that builds the index definition string
  - : Converts C string to PostgreSQL text type
  - : Returns text value to SQL caller
  - : Returns NULL to SQL caller
- Called from (representative examples):
  - SQL queries using pg_get_indexdef(oid, int, boolean) function
  - System administration tools requiring specific column expressions
  - Database introspection utilities

## Notes and Other Information
- Provides more flexibility than the basic pg_get_indexdef function
- Can return individual column expressions instead of complete definition
- Supports pretty-printing control for output formatting
- The colno parameter enables extraction of specific index key expressions
- Still excludes tablespace information like the basic version
- Part of PostgreSQL's rule utilities system for reconstructing DDL statements
- Located in src/backend/utils/adt/ruleutils.c:1178-1204
- Useful for analyzing complex index expressions and functional indexes