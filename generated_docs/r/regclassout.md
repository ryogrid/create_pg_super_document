# regclassout

## Location
[src/backend/utils/adt/regproc.c:943-999](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regproc.c#L943-L999)

## Overview
Converts a relation OID to its human-readable class name string representation, with proper namespace qualification when necessary.

## Definition

```c
Datum
regclassout(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is an output function for the  data type that converts a relation OID (Object Identifier) into its corresponding class name string. This function is part of PostgreSQL's registry type system that provides user-friendly representations of internal object identifiers.

The function performs the following operations:
1. Extracts the OID from the function arguments
2. Handles the special case of InvalidOid by returning "-"
3. Looks up the relation in the system catalog ()
4. Determines if namespace qualification is needed based on search path visibility
5. Returns either a simple class name or a schema-qualified name
6. Falls back to numeric representation if the OID is not found in pg_class

The function intelligently handles namespace qualification - it only includes the schema name when the relation would not be found by name alone due to search path rules.

## Parameters / Member Variables
- Input:  (Oid) - The relation OID to be converted to string format

## Dependencies
- Functions called/Symbols referenced:
  -  - Extract OID argument from function call
  -  - Look up relation in system cache
  -  - Check if catalog lookup succeeded  
  -  - Extract structure from heap tuple
  -  - Check if in bootstrap mode
  -  - Determine if relation is visible in current search path
  -  - Get schema name for namespace OID
  -  - Properly quote and qualify identifiers
  -  - Release system cache reference
  -  - Return C string result

- Called from (representative examples):
  -  (in xml.c)

## Notes and Other Information
- Returns "-" for InvalidOid as a special marker
- In bootstrap processing mode, skips namespace qualification for simplicity
- Only qualifies names with schema when necessary for disambiguation
- Falls back to numeric representation when OID doesn't exist in pg_class
- Part of the regclass data type I/O functions alongside regclassin
- Uses the system cache for efficient catalog lookups
- Properly handles memory management with palloc and pstrdup

## Simplified Source

```c
Datum
regclassout(PG_FUNCTION_ARGS)
{
    Oid classid = PG_GETARG_OID(0);
    char *result;
    HeapTuple classtup;

    // Handle invalid OID special case
    if (classid == InvalidOid) {
        result = pstrdup("-");
        PG_RETURN_CSTRING(result);
    }

    // Look up relation in system catalog
    classtup = SearchSysCache1(RELOID, ObjectIdGetDatum(classid));

    if (HeapTupleIsValid(classtup)) {
        Form_pg_class classform = (Form_pg_class) GETSTRUCT(classtup);
        char *classname = NameStr(classform->relname);

        // In bootstrap mode, return simple name
        if (IsBootstrapProcessingMode())
            result = pstrdup(classname);
        else {
            // Check if namespace qualification needed
            char *nspname = NULL;
            if (!RelationIsVisible(classid))
                nspname = get_namespace_name(classform->relnamespace);

            result = quote_qualified_identifier(nspname, classname);
        }
        ReleaseSysCache(classtup);
    } else {
        // Return numeric representation if not found in catalog
        result = (char *) palloc(NAMEDATALEN);
        snprintf(result, NAMEDATALEN, "%u", classid);
    }

    PG_RETURN_CSTRING(result);
}
```