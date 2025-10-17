# get_type_io_data

## Location
[src/backend/utils/cache/lsyscache.c:2325-2398](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L2325-L2398)

## Overview
Retrieves type metadata and I/O function information for a given PostgreSQL data type OID, including type length, alignment, delimiter, and the appropriate I/O function based on the requested operation.

## Definition

```c
void
get_type_io_data(Oid typid,
				 IOFuncSelector which_func,
				 int16 *typlen,
				 bool *typbyval,
				 char *typalign,
				 char *typdelim,
				 Oid *typioparam,
				 Oid *func)
```
## Detailed Description
This function serves as a comprehensive interface for retrieving type-related metadata from the PostgreSQL system catalog (pg_type). It extracts six key pieces of information about a data type in a single call, making it efficient for operations that need multiple type attributes simultaneously. The function handles both normal operation mode and bootstrap mode, where it delegates to  for basic types during system initialization. The I/O function returned depends on the  parameter, allowing callers to specify whether they need input, output, receive, or send functions.

## Parameters / Member Variables
- `typid`: The OID of the data type to look up in the system catalog
- `which_func`: Selector specifying which I/O function to return (input, output, receive, or send)
- `*typlen`: Output parameter for the type's storage length (-1 for variable length types)
- `*typbyval`: Output parameter indicating whether the type is passed by value or reference
- `*typalign`: Output parameter for the type's alignment requirement ('c', 's', 'i', or 'd')
- `*typdelim`: Output parameter for the type's array element delimiter character
- `*typioparam`: Output parameter for the type's I/O parameter OID
- `*func`: Output parameter for the requested I/O function OID
## Dependencies
- Functions called/Symbols referenced:
  - IsBootstrapProcessingMode
  - [boot_get_type_io_data](../b/boot_get_type_io_data.md)
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - [getTypeIOParam](getTypeIOParam.md)
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - GETSTRUCT
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - HeapTupleIsValid
- Called from (representative examples):
  - [array_in](../a/array_in.md)
  - [array_out](../a/array_out.md)
  - [array_recv](../a/array_recv.md)
  - [array_send](../a/array_send.md)
  - [get_range_io_data](get_range_io_data.md)
  - [get_multirange_io_data](get_multirange_io_data.md)

## Notes and Other Information
- The function performs a system catalog lookup using the syscache for efficient access to type information
- During bootstrap mode, only input and output functions are supported; binary I/O functions (receive/send) will cause an error
- The function will throw an ERROR if the type OID is not found in the system catalog
- This is part of the lsyscache.c module, which provides cached access to system catalog information
- The returned type information is essential for proper serialization, deserialization, and storage of PostgreSQL data types

## Simplified Source

```c
void get_type_io_data(Oid typid, IOFuncSelector which_func,
                     int16 *typlen, bool *typbyval, char *typalign,
                     char *typdelim, Oid *typioparam, Oid *func) {

    // Handle bootstrap mode with special processing
    if (IsBootstrapProcessingMode()) {
        Oid typinput, typoutput;
        boot_get_type_io_data(typid, typlen, typbyval, typalign,
                             typdelim, typioparam, &typinput, &typoutput);

        // Select appropriate function for bootstrap mode
        switch (which_func) {
            case IOFunc_input:  *func = typinput; break;
            case IOFunc_output: *func = typoutput; break;
            default:
                elog(ERROR, "binary I/O not supported during bootstrap");
        }
        return;
    }

    // Normal mode: look up type in system cache
    HeapTuple typeTuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
    if (!HeapTupleIsValid(typeTuple))
        elog(ERROR, "cache lookup failed for type %u", typid);

    Form_pg_type typeStruct = (Form_pg_type) GETSTRUCT(typeTuple);

    // Extract all type attributes
    *typlen = typeStruct->typlen;
    *typbyval = typeStruct->typbyval;
    *typalign = typeStruct->typalign;
    *typdelim = typeStruct->typdelim;
    *typioparam = getTypeIOParam(typeTuple);

    // Select requested I/O function
    switch (which_func) {
        case IOFunc_input:   *func = typeStruct->typinput; break;
        case IOFunc_output:  *func = typeStruct->typoutput; break;
        case IOFunc_receive: *func = typeStruct->typreceive; break;
        case IOFunc_send:    *func = typeStruct->typsend; break;
    }

    ReleaseSysCache(typeTuple);
}
```

This simplified version shows the function's dual-mode operation: bootstrap mode with limited I/O function support, and normal mode with complete type metadata retrieval. It efficiently extracts six type attributes in one catalog lookup and selects the appropriate I/O function based on the caller's needs.