# regdictionaryout

## Location
[src/backend/utils/adt/regproc.c:1469-1515](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regproc.c#L1469-L1515)

## Overview
Converts a text search dictionary OID to its corresponding dictionary name string for output display.

## Definition

```c
Datum
regdictionaryout(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is part of PostgreSQL's regtype system for text search dictionaries. It takes an OID (Object Identifier) representing a text search dictionary and converts it to a human-readable string representation. The function handles multiple output scenarios based on the dictionary's visibility and existence:

1. **Invalid OID**: Returns "-" for  
2. **Valid, visible dictionary**: Returns just the dictionary name if it's in the current search path
3. **Valid, non-visible dictionary**: Returns schema-qualified name (e.g., "schema.dictname")
4. **Non-existent OID**: Returns the numeric OID as a string

The function uses the system catalog  to look up dictionary details and applies PostgreSQL's visibility rules to determine whether schema qualification is necessary for unambiguous identification.

## Parameters / Member Variables
- **Input**: OID of the text search dictionary (accessed via )
- **Return**:  containing a C string representation of the dictionary

## Dependencies
- Functions called/Symbols referenced:
  -  - Extract OID argument from function call
  -  - Search system catalog cache for dictionary entry
  -  - Extract tuple structure from heap tuple
  -  - Check if dictionary is visible in current search path
  -  - Get schema name from namespace OID
  -  - Properly quote and qualify identifiers
  -  - Release system cache tuple
  -  - Return string result
- Called from:
  - System catalog output functions (indirectly via SQL system)

## Notes and Other Information
- This function is the output counterpart to  which parses dictionary names
- Uses PostgreSQL's visibility rules to determine when schema qualification is needed
- Falls back to numeric representation for non-existent OIDs rather than throwing an error
- Memory management uses  for result allocation
- Part of the regtype system that provides user-friendly representations of internal OIDs
- Critical for displaying dictionary references in SQL output and system catalogs

## Simplified Source

```c
Datum
regdictionaryout(PG_FUNCTION_ARGS)
{
    Oid dict_id = PG_GETARG_OID(0);
    char *result;

    // Handle invalid OID
    if (dict_id == InvalidOid) {
        result = pstrdup("-");
        PG_RETURN_CSTRING(result);
    }

    // Look up dictionary in system catalog
    HeapTuple dict_tuple = SearchSysCache1(TSDICTOID, ObjectIdGetDatum(dict_id));

    if (HeapTupleIsValid(dict_tuple)) {
        // Extract dictionary details
        Form_pg_ts_dict dict_form = (Form_pg_ts_dict) GETSTRUCT(dict_tuple);
        char *dict_name = NameStr(dict_form->dictname);
        char *namespace_name;

        // Check if schema qualification needed
        if (TSDictionaryIsVisible(dict_id)) {
            namespace_name = NULL;
        } else {
            namespace_name = get_namespace_name(dict_form->dictnamespace);
        }

        result = quote_qualified_identifier(namespace_name, dict_name);
        ReleaseSysCache(dict_tuple);
    } else {
        // Return numeric OID if not found in catalog
        result = (char *) palloc(NAMEDATALEN);
        snprintf(result, NAMEDATALEN, "%u", dict_id);
    }

    PG_RETURN_CSTRING(result);
}
```