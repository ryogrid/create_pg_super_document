# format_type_extended

## Location
[src/backend/utils/adt/format_type.c:112-342](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/format_type.c#L112-L342)

## Overview
Core internal function that generates possibly-qualified PostgreSQL type names with extensive formatting control through flag-based options.

## Definition

```c
structed.  Also check the toast property, and don't
	 * deconstruct "plain storage" array types --- this is because we don't
	 * want to show oidvector as oid[].
	 */
	array_base_type = typeform->typelem;
```
## Detailed Description
 is the main workhorse function for PostgreSQL type name formatting. It provides comprehensive control over how type names are presented through a flags parameter. The function handles built-in types with special formatting rules, array types, schema qualification decisions, and various error conditions.

The function implements special formatting for standard PostgreSQL types (like converting FLOAT4OID to "real", INT4OID to "integer") to ensure output matches SQL standard names. For non-standard types, it uses the catalog name with appropriate quoting and qualification.

Key behaviors controlled by flags:
- **FORMAT_TYPE_TYPEMOD_GIVEN**: Include typemod in output even if it's -1
- **FORMAT_TYPE_ALLOW_INVALID**: Return "???" for invalid OIDs instead of erroring  
- **FORMAT_TYPE_INVALID_AS_NULL**: Return NULL for invalid OIDs
- **FORMAT_TYPE_FORCE_QUALIFY**: Always include schema qualification

Array handling is sophisticated - it detects "true" array types while avoiding pseudo-arrays like "name" and checks storage properties to avoid showing internal types like oidvector as oid[].

## Parameters / Member Variables
- : PostgreSQL type OID from pg_type.oid
- : Type modifier value, -1 indicates no specific modifier
- : Bitfield controlling formatting behavior (FORMAT_TYPE_* constants)

## Dependencies
- Functions called/Symbols referenced:
  -  - System catalog lookup for type information
  -  - Determines if type is a genuine array type
  -  - Formats type modifiers for display
  -  - Checks if type is in current search path
  -  - Gets schema name for qualification
  -  - Properly quotes and qualifies identifiers
  -  - Safe string formatting

- Called from (representative examples):
  -  - Primary SQL function interface
  -  - [Backend](../B/Backend.md)-only variant
  -  - Always-qualified variant
  -  - Typemod-included variant
  -  - Object description generation
  -  - Array input processing

## Notes and Other Information
- Returns a palloc'd string that must be freed by the caller, or NULL for invalid types when appropriate flags are set
- Contains extensive special-case handling for built-in types to ensure SQL standard compliance
- The array detection logic carefully avoids pseudo-arrays to prevent confusing output
- Schema qualification logic respects search_path unless forced
- Critical for maintaining consistency in pg_dump output and ensuring DDL reconstruction accuracy
- Handles edge cases like bit(-1) vs BIT and bpchar(-1) vs CHARACTER to maintain parser compatibility

## Simplified Source

```c
char *
format_type_extended(Oid type_oid, int32 typemod, bits16 flags)
{
    HeapTuple tuple;
    Form_pg_type typeform;
    Oid array_base_type;
    bool is_array;
    char *buf;
    bool with_typemod;

    // Handle invalid type OID based on flags
    if (type_oid == InvalidOid) {
        if ((flags & FORMAT_TYPE_INVALID_AS_NULL) != 0)
            return NULL;
        else if ((flags & FORMAT_TYPE_ALLOW_INVALID) != 0)
            return pstrdup("-");
    }

    // Look up type information in system catalog
    tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type_oid));
    if (!HeapTupleIsValid(tuple)) {
        // Handle lookup failure based on flags
        if ((flags & FORMAT_TYPE_INVALID_AS_NULL) != 0)
            return NULL;
        else if ((flags & FORMAT_TYPE_ALLOW_INVALID) != 0)
            return pstrdup("???");
        else
            elog(ERROR, "cache lookup failed for type %u", type_oid);
    }
    typeform = (Form_pg_type) GETSTRUCT(tuple);

    // Check if this is a true array type (not pseudo-arrays like "name")
    array_base_type = typeform->typelem;
    if (IsTrueArrayType(typeform) && typeform->typstorage != TYPSTORAGE_PLAIN) {
        // Switch focus to array element type
        ReleaseSysCache(tuple);
        tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(array_base_type));
        if (!HeapTupleIsValid(tuple)) {
            // Handle array element lookup failure
            return handle_invalid_type(flags, "???[]");
        }
        typeform = (Form_pg_type) GETSTRUCT(tuple);
        type_oid = array_base_type;
        is_array = true;
    } else {
        is_array = false;
    }

    with_typemod = (flags & FORMAT_TYPE_TYPEMOD_GIVEN) != 0 && (typemod >= 0);

    // Handle built-in types with special formatting
    buf = NULL;
    switch (type_oid) {
        case BITOID:
            buf = with_typemod ? printTypmod("bit", typemod, typeform->typmodout) : pstrdup("bit");
            break;
        case BOOLOID:
            buf = pstrdup("boolean");
            break;
        case BPCHAROID:
            buf = with_typemod ? printTypmod("character", typemod, typeform->typmodout) : pstrdup("character");
            break;
        case FLOAT4OID:
            buf = pstrdup("real");
            break;
        case FLOAT8OID:
            buf = pstrdup("double precision");
            break;
        case INT2OID:
            buf = pstrdup("smallint");
            break;
        case INT4OID:
            buf = pstrdup("integer");
            break;
        case INT8OID:
            buf = pstrdup("bigint");
            break;
        case NUMERICOID:
            buf = with_typemod ? printTypmod("numeric", typemod, typeform->typmodout) : pstrdup("numeric");
            break;
        // ... other built-in types follow same pattern
    }

    // Default handling for non-built-in types
    if (buf == NULL) {
        char *nspname;
        char *typname;

        // Determine if schema qualification is needed
        if ((flags & FORMAT_TYPE_FORCE_QUALIFY) == 0 && TypeIsVisible(type_oid))
            nspname = NULL;
        else
            nspname = get_namespace_name_or_temp(typeform->typnamespace);

        typname = NameStr(typeform->typname);
        buf = quote_qualified_identifier(nspname, typname);

        if (with_typemod)
            buf = printTypmod(buf, typemod, typeform->typmodout);
    }

    // Add array notation if needed
    if (is_array)
        buf = psprintf("%s[]", buf);

    ReleaseSysCache(tuple);
    return buf;
}
```