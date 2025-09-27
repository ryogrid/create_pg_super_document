# getTypeInputInfo

## Location
[src/backend/utils/cache/lsyscache.c:2874-2906](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L2874-L2906)

## Overview
Retrieves the input function and I/O parameter information needed for converting string representations of values to a type's internal form, serving as a core component of PostgreSQL's type input conversion system.

## Definition
```c
void getTypeInputInfo(Oid type, Oid *typInput, Oid *typIOParam)
```

## Detailed Description
This function performs a system catalog lookup to obtain the essential information needed for converting external string representations of values into a type's internal binary format. It retrieves two critical pieces of information: the OID of the type's input function (`typinput`) and the I/O parameter (`typioparam`) that should be passed to that function.

The function performs several validation checks to ensure the type is properly defined and usable:
1. Verifies the type exists in the system catalog
2. Checks that the type is fully defined (not just a shell type)
3. Ensures the type has a valid input function

If any of these conditions fail, the function raises appropriate errors. The I/O parameter is obtained by calling `getTypeIOParam()`, which handles the logic for determining the correct parameter based on the type's characteristics.

This function is fundamental to PostgreSQL's type system and is used extensively throughout the codebase wherever values need to be converted from their external string representation to internal format, such as during query parsing, data loading, and inter-process communication.

## Parameters / Member Variables
- `type`: The OID of the type for which input information is needed
- `typInput`: Output parameter that receives the OID of the type's input function
- `typIOParam`: Output parameter that receives the I/O parameter to be passed to the input function

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - HeapTupleIsValid
  - GETSTRUCT
  - Form_pg_type
  - elog
  - ereport
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
  - [format_type_be](../f/format_type_be.md)
  - OidIsValid
  - [getTypeIOParam](getTypeIOParam.md)
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
- Called from (representative examples):
  - [DefineAggregate](../D/DefineAggregate.md)
  - [BeginCopyFrom](../B/BeginCopyFrom.md)
  - [ExecInitExprRec](../E/ExecInitExprRec.md)
  - [TupleDescGetAttInMetadata](../T/TupleDescGetAttInMetadata.md)
  - [GetAggInitVal](../G/GetAggInitVal.md)
  - [exec_bind_message](../e/exec_bind_message.md)
  - [record_in](../r/record_in.md)
  - [PLy_output_setup_func](../P/PLy_output_setup_func.md)

## Notes and Other Information
- Raises ERROR if the type does not exist in the system catalog
- Raises ERROR with ERRCODE_UNDEFINED_OBJECT if the type is only a shell (not fully defined)
- Raises ERROR with ERRCODE_UNDEFINED_FUNCTION if the type lacks a valid input function
- Uses the system cache (TYPEOID cache) for efficient type catalog lookups
- The `typIOParam` is determined by `getTypeIOParam()` which handles various type-specific parameter logic
- Essential for all operations that need to convert string representations to internal type values
- Part of the core type system infrastructure in PostgreSQL's lsyscache module

## Simplified Source

```c
// Simplified version of getTypeInputInfo
void getTypeInputInfo(Oid type, Oid *typInput, Oid *typIOParam) {
    HeapTuple typeTuple;
    Form_pg_type typeForm;

    // Look up the type in the system catalog
    typeTuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type));
    if (!HeapTupleIsValid(typeTuple)) {
        elog(ERROR, "cache lookup failed for type %u", type);
    }

    // Get the type structure from the tuple
    typeForm = (Form_pg_type) GETSTRUCT(typeTuple);

    // Validate that the type is fully defined and has an input function
    if (!typeForm->typisdefined) {
        ereport(ERROR, "type is only a shell");
    }
    if (!OidIsValid(typeForm->typinput)) {
        ereport(ERROR, "no input function available for type");
    }

    // Return the input function OID and I/O parameter
    *typInput = typeForm->typinput;
    *typIOParam = getTypeIOParam(typeTuple);

    // Clean up the cache reference
    ReleaseSysCache(typeTuple);
}
```

Key simplifications made:
- Simplified error messages for clarity (removed detailed error codes and formatting)
- Used more descriptive variable name (`typeForm` instead of `pt`)
- Condensed the validation logic while preserving all essential checks
- Focused on the main execution path: lookup → validate → extract → cleanup
- Maintained the core algorithm structure and all critical functionality