# pg_get_constraintdef_ext

## Location
[src/backend/utils/adt/ruleutils.c:2143-2163](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L2143-L2163)

## Overview
This function provides an extended interface for retrieving the SQL definition of a database constraint, with control over pretty-printing format.

## Definition

```c
Datum
pg_get_constraintdef_ext(PG_FUNCTION_ARGS)
```
## Detailed Description
pg_get_constraintdef_ext is a PostgreSQL built-in function that returns the SQL definition text for a specified constraint. It extends the basic constraint definition retrieval by allowing the caller to control whether the output should be formatted for readability (pretty-printed) or kept in a compact form. This function serves as a wrapper around pg_get_constraintdef_worker, providing a user-accessible interface with formatting options.

## Parameters / Member Variables
-  (Oid): The object identifier of the constraint to retrieve the definition for
-  (bool): Flag indicating whether the output should be pretty-printed for readability

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_OID (macro for extracting OID argument)
  - PG_GETARG_BOOL (macro for extracting boolean argument)
  - GET_PRETTY_FLAGS (macro for converting boolean to pretty-printing flags)
  - [pg_get_constraintdef_worker](pg_get_constraintdef_worker.md) (core worker function that generates constraint definition)
  - [string_to_text](../s/string_to_text.md) (utility function for converting C string to PostgreSQL text type)
  - PG_RETURN_TEXT_P (macro for returning text result)
  - PG_RETURN_NULL (macro for returning NULL result)
- Called from:
  - This function is typically called from SQL queries as a built-in function

## Notes and Other Information
- This function is exposed to SQL users as pg_get_constraintdef(oid, boolean)
- Returns NULL if the constraint with the given OID does not exist
- The pretty-printing option affects formatting such as line breaks and indentation in complex constraint definitions
- Located in src/backend/utils/adt/ruleutils.c:2143-2163

## Simplified Source

```c
Datum
pg_get_constraintdef_ext(PG_FUNCTION_ARGS)
{
    Oid constraintId = PG_GETARG_OID(0);
    bool pretty = PG_GETARG_BOOL(1);
    char *result;

    // Convert pretty flag to formatting flags
    int prettyFlags = GET_PRETTY_FLAGS(pretty);

    // Generate constraint definition with specified formatting
    result = pg_get_constraintdef_worker(constraintId, false, prettyFlags, true);

    if (result == NULL)
        PG_RETURN_NULL();

    PG_RETURN_TEXT_P(string_to_text(result));
}
```