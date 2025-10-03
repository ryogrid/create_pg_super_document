# get_am_type_oid

## Location
[src/backend/commands/amcmds.c:129-162](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/amcmds.c#L129-L162)

## Overview
Internal worker function that looks up an access method by name and optionally validates its type, returning the corresponding OID.

## Definition

```c
static Oid
get_am_type_oid(const char *amname, char amtype, bool missing_ok)
```
## Detailed Description
get_am_type_oid serves as a core utility function for access method OID lookups with optional type validation. It searches the pg_am system catalog for an access method by name and can enforce type constraints when specified. The function supports both strict mode (throwing errors for missing access methods) and lenient mode (returning InvalidOid), making it suitable for various use cases throughout the access method subsystem.

## Parameters / Member Variables
- `*amname`: Name of the access method to look up
- `amtype`: Expected access method type character ('\0' to skip type validation)
- `missing_ok`: If false, throws error when access method not found; if true, returns InvalidOid
## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md): Searches system cache for access method by name
  - [CStringGetDatum](../C/CStringGetDatum.md): Converts C string to Datum for cache lookup
  - GETSTRUCT: Macro to extract structure from heap tuple
  - [get_am_type_string](get_am_type_string.md): Converts access method type character to string representation
  - [ReleaseSysCache](../R/ReleaseSysCache.md): Releases system cache tuple
- Called from (representative examples):
  - [get_index_am_oid](get_index_am_oid.md): Gets OID for index access methods
  - [get_table_am_oid](get_table_am_oid.md): Gets OID for table access methods
  - [get_am_oid](get_am_oid.md): Gets OID for any access method type

## Notes and Other Information
- This is a static function used internally within the access method command subsystem
- Provides centralized access method lookup logic with type validation
- Uses system cache for efficient repeated lookups
- Error handling distinguishes between missing access methods and type mismatches
- Location: src/backend/commands/amcmds.c:129-162

## Simplified Source

```c
static Oid get_am_type_oid(const char *amname, char amtype, bool missing_ok)
{
    HeapTuple tup;
    Oid oid = InvalidOid;

    // Look up access method by name
    tup = SearchSysCache1(AMNAME, CStringGetDatum(amname));
    if (HeapTupleIsValid(tup))
    {
        Form_pg_am amform = (Form_pg_am) GETSTRUCT(tup);

        // Validate access method type if specified
        if (amtype != '\0' && amform->amtype != amtype)
            ereport(ERROR,
                    (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                     errmsg("access method \"%s\" is not of type %s",
                            NameStr(amform->amname),
                            get_am_type_string(amtype))));

        oid = amform->oid;
        ReleaseSysCache(tup);
    }

    // Handle missing access method
    if (!OidIsValid(oid) && !missing_ok)
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                 errmsg("access method \"%s\" does not exist", amname)));

    return oid;
}
```