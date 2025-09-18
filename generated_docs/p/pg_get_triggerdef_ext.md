# pg_get_triggerdef_ext

## Location
src/backend/utils/adt/ruleutils.c: 865 - 879

## Overview
A SQL-callable function that returns the complete CREATE TRIGGER statement for a given trigger OID with formatting options.

## Definition


## Detailed Description
This function serves as the main entry point for retrieving trigger definitions from PostgreSQL's system catalogs. It accepts a trigger OID and a boolean flag for pretty-printing, then delegates the actual work to . The function handles the conversion between PostgreSQL's internal C representation and the SQL text type that can be returned to SQL queries. If the trigger is not found, it returns NULL.

## Parameters / Member Variables
- : The OID of the trigger to retrieve the definition for
- : Boolean flag indicating whether to format the output for readability (pretty-printing)

## Dependencies
- Functions called/Symbols referenced:
  - : Extracts OID argument from function call
  - : Extracts boolean argument from function call  
  - : Core function that builds the trigger definition string
  - : Converts C string to PostgreSQL text type
  - : Returns text value to SQL caller
  - : Returns NULL to SQL caller
- Called from (representative examples):
  - SQL queries using pg_get_triggerdef(oid, boolean) function
  - System administration and introspection tools

## Notes and Other Information
- This is a wrapper function that provides the SQL interface to trigger definition retrieval
- The actual trigger definition construction is handled by the static worker function
- Part of PostgreSQL's rule utilities system for reconstructing DDL statements
- Located in src/backend/utils/adt/ruleutils.c:865-879
- The pretty-printing option affects schema qualification and formatting of the output