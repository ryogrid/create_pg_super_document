# boot_get_type_io_data

## Location
[src/backend/bootstrap/bootstrap.c:806-882](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/bootstrap/bootstrap.c#L806-L882)

## Overview
A function that retrieves type I/O metadata during PostgreSQL bootstrap, providing an early-bootstrap equivalent to the standard get_type_io_data function with limited functionality.

## Definition

```c
struct typmap *ap = NULL;
```
## Detailed Description
This function obtains essential type information needed for input/output operations during the bootstrap phase of PostgreSQL initialization. It operates in two modes depending on whether the pg_type catalog has been loaded:

1. **Post-catalog mode**: When Typ list is populated, it searches the cached pg_type contents for the requested type and extracts all I/O metadata from the type's catalog entry.

2. **Pre-catalog mode**: When Typ is NIL, it falls back to the hard-coded TypInfo array that contains essential bootstrap types, using simplified assumptions (like comma delimiter for all types).

The function is designed to support array_in and array_out operations during early bootstrap when the full type system is not yet available. Unlike the full get_type_io_data function, it only provides text I/O routines, not binary I/O routines.

## Parameters / Member Variables
- : The OID of the type to look up
- : Output parameter for type length (-1 for variable length)
- : Output parameter indicating if type is passed by value
- : Output parameter for type alignment requirement ('c', 's', 'i', 'd')
- : Output parameter for array element delimiter character
- : Output parameter for I/O parameter (element type for arrays, self for others)
- : Output parameter for input function OID
- : Output parameter for output function OID

## Dependencies
- Functions called/Symbols referenced:
  - elog (for error reporting)
  - OidIsValid (to check if OID is valid)
  - [typmap](../t/typmap.md) (structure for cached type information)

- Called from:
  - [InsertOneValue](../I/InsertOneValue.md) (during bootstrap data insertion)
  - [get_type_io_data](../g/get_type_io_data.md) (as fallback in lsyscache)

## Notes and Other Information
- Exported function (not static) to support array I/O operations during bootstrap
- API intentionally matches lsyscache.c's get_type_io_data for compatibility
- Only supports text I/O routines (typinput/typoutput), not binary I/O
- Uses simplified logic for pre-catalog phase (assumes comma delimiter)
- The typioparam logic matches getTypeIOParam() for consistency
- Essential for enabling array operations before the full type system is ready
- Falls back gracefully between cached pg_type data and hard-coded TypInfo array

## Simplified Source

```c
void boot_get_type_io_data(Oid typid, int16 *typlen, bool *typbyval,
                          char *typalign, char *typdelim, Oid *typioparam,
                          Oid *typinput, Oid *typoutput) {
    if (Typ != NIL) {
        // Search cached pg_type data
        struct typmap *ap = NULL;
        ListCell *lc;
        foreach(lc, Typ) {
            ap = lfirst(lc);
            if (ap->am_oid == typid)
                break;
        }

        if (!ap || ap->am_oid != typid)
            elog(ERROR, "type OID %u not found in Typ list", typid);

        // Extract type information from pg_type entry
        *typlen = ap->am_typ.typlen;
        *typbyval = ap->am_typ.typbyval;
        *typalign = ap->am_typ.typalign;
        *typdelim = ap->am_typ.typdelim;
        *typinput = ap->am_typ.typinput;
        *typoutput = ap->am_typ.typoutput;

        // Set I/O parameter (element type for arrays, self for others)
        if (OidIsValid(ap->am_typ.typelem))
            *typioparam = ap->am_typ.typelem;
        else
            *typioparam = typid;
    } else {
        // Use hardcoded TypInfo array
        int typeindex;
        for (typeindex = 0; typeindex < n_types; typeindex++) {
            if (TypInfo[typeindex].oid == typid)
                break;
        }
        if (typeindex >= n_types)
            elog(ERROR, "type OID %u not found in TypInfo", typid);

        // Extract type information from TypInfo entry
        *typlen = TypInfo[typeindex].len;
        *typbyval = TypInfo[typeindex].byval;
        *typalign = TypInfo[typeindex].align;
        *typdelim = ',';  // Assume comma delimiter for all bootstrap types
        *typinput = TypInfo[typeindex].inproc;
        *typoutput = TypInfo[typeindex].outproc;

        // Set I/O parameter (element type for arrays, self for others)
        if (OidIsValid(TypInfo[typeindex].elem))
            *typioparam = TypInfo[typeindex].elem;
        else
            *typioparam = typid;
    }
}
```