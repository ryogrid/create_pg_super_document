# pg_get_triggerdef_ext

## Location
[src/backend/utils/adt/ruleutils.c:865-879](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L865-L879)

## Overview
A SQL-callable function that returns the complete CREATE TRIGGER statement for a given trigger OID with formatting options.

## Definition

```c
Datum
pg_get_triggerdef_ext(PG_FUNCTION_ARGS)
```
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

## Simplified Source

```c
Datum pg_get_triggerdef_ext(PG_FUNCTION_ARGS) {
    // Extract trigger OID and formatting preference
    Oid trigid = PG_GETARG_OID(0);
    bool pretty = PG_GETARG_BOOL(1);

    // Get trigger definition from worker function
    char *res = pg_get_triggerdef_worker(trigid, pretty);

    // Return NULL if trigger not found, otherwise convert to text
    if (res == NULL)
        PG_RETURN_NULL();

    PG_RETURN_TEXT_P(string_to_text(res));
}
```