# regcollationout

## Location
[src/backend/utils/adt/regproc.c:1086-1143](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regproc.c#L1086-L1143)

## Overview
Converts a collation OID to its corresponding collation name string representation in human-readable format.

## Definition

```c
Datum
regcollationout(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is an output function for the regcollation data type that converts a collation OID (Object Identifier) to a human-readable string representation. It performs the following operations:

1. **Invalid OID handling**: Returns "-" for InvalidOid input
2. **System catalog lookup**: Searches the pg_collation system catalog to find the collation entry
3. **Namespace resolution**: Determines if the collation name needs to be schema-qualified based on visibility rules
4. **Bootstrap mode support**: In bootstrap processing mode, returns only the collation name without namespace qualification
5. **Fallback handling**: If no matching catalog entry is found, returns the OID as a numeric string

The function ensures that the output string can be parsed back by  by applying appropriate schema qualification when necessary.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro containing:
  -  (Oid): The collation OID to be converted to string format

## Dependencies
- Functions called/Symbols referenced:
  - : Extract OID argument from function call
  - : Look up collation in system cache using COLLOID
  - : Cast heap tuple to collation form structure
  - : Check if running in bootstrap mode
  - : Determine if collation is visible in current search path
  - : Get namespace name from namespace OID
  - : Create properly quoted schema.name identifier
  - : Return C string as PostgreSQL Datum
- Called from:
  - Output function for regcollation type (referenced in system catalogs)

## Notes and Other Information
- This function is part of the regproc family of functions that handle object identifier to name conversions
- The function handles namespace visibility to ensure the output can be reparsed correctly
- In bootstrap mode, namespace qualification is skipped for simplicity
- Memory allocation uses PostgreSQL's palloc system for proper memory management
- The function is typically used internally by PostgreSQL when displaying regcollation values in query results or system views

## Simplified Source

```c
Datum
regcollationout(PG_FUNCTION_ARGS)
{
    Oid collationid = PG_GETARG_OID(0);
    char *result;
    HeapTuple collationtup;

    // Handle invalid OID special case
    if (collationid == InvalidOid) {
        result = pstrdup("-");
        PG_RETURN_CSTRING(result);
    }

    // Look up collation in system catalog
    collationtup = SearchSysCache1(COLLOID, ObjectIdGetDatum(collationid));

    if (HeapTupleIsValid(collationtup)) {
        Form_pg_collation collationform = (Form_pg_collation) GETSTRUCT(collationtup);
        char *collationname = NameStr(collationform->collname);

        // In bootstrap mode, return simple name
        if (IsBootstrapProcessingMode())
            result = pstrdup(collationname);
        else {
            // Check if namespace qualification needed
            char *nspname = NULL;
            if (!CollationIsVisible(collationid))
                nspname = get_namespace_name(collationform->collnamespace);

            result = quote_qualified_identifier(nspname, collationname);
        }
        ReleaseSysCache(collationtup);
    } else {
        // Return numeric representation if not found in catalog
        result = (char *) palloc(NAMEDATALEN);
        snprintf(result, NAMEDATALEN, "%u", collationid);
    }

    PG_RETURN_CSTRING(result);
}
```