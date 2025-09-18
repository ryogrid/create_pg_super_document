# getTypeOutputInfo

## Location
[src/backend/utils/cache/lsyscache.c:2907-2939](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L2907-L2939)

## Overview
Retrieves the output function and variable-length information needed for converting values from a type's internal form to printable string representation, forming the counterpart to input conversion in PostgreSQL's type system.

## Definition
```c
void getTypeOutputInfo(Oid type, Oid *typOutput, bool *typIsVarlena)
```

## Detailed Description
This function performs a system catalog lookup to obtain essential information for converting values from their internal binary format to external string representation for display or transmission. It retrieves two critical pieces of information: the OID of the type's output function (`typoutput`) and whether the type is variable-length (`typIsVarlena`).

The function performs the same validation checks as its input counterpart:
1. Verifies the type exists in the system catalog
2. Checks that the type is fully defined (not just a shell type)
3. Ensures the type has a valid output function

The `typIsVarlena` flag is computed by checking if the type is not passed by value (`!pt->typbyval`) and has variable length (`pt->typlen == -1`). This information is crucial for memory management and optimization decisions when handling the type's values.

This function is essential for all operations that need to display, print, or transmit type values in human-readable format, including query result formatting, error messages, logging, and client communication.

## Parameters / Member Variables
- `type`: The OID of the type for which output information is needed
- `typOutput`: Output parameter that receives the OID of the type's output function
- `typIsVarlena`: Output parameter that receives whether the type is variable-length

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
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
- Called from (representative examples):
  - [printtup_prepare_info](../p/printtup_prepare_info.md)
  - [DoCopyTo](../D/DoCopyTo.md)
  - [ExecBuildSlotValueDescription](../E/ExecBuildSlotValueDescription.md)
  - [SPI_getvalue](../S/SPI_getvalue.md)
  - [json_categorize_type](../j/json_categorize_type.md)
  - [record_out](../r/record_out.md)
  - [text_format](../t/text_format.md)
  - [PLy_input_setup_func](../P/PLy_input_setup_func.md)

## Notes and Other Information
- Raises ERROR if the type does not exist in the system catalog
- Raises ERROR with ERRCODE_UNDEFINED_OBJECT if the type is only a shell (not fully defined)
- Raises ERROR with ERRCODE_UNDEFINED_FUNCTION if the type lacks a valid output function
- The `typIsVarlena` flag is computed as `(!pt->typbyval) && (pt->typlen == -1)`
- Uses the system cache (TYPEOID cache) for efficient type catalog lookups
- Complementary function to `getTypeInputInfo` - together they provide complete type conversion capabilities
- Essential for query result formatting, error reporting, and all client-server communication involving type values
- Part of the core type system infrastructure in PostgreSQL's lsyscache module