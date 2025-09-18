# sqlda_native_empty_size

## Location
[src/interfaces/ecpg/ecpglib/sqlda.c:171-185](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/sqlda.c#L171-L185)

## Overview
Calculates the minimum memory size required for an empty native-mode SQLDA structure that can hold metadata for a given PostgreSQL result set.

## Definition


## Detailed Description
This function computes the base memory requirements for a native-mode SQLDA structure before any actual data values are stored. It calculates the space needed for the main sqlda_struct and the required number of sqlvar_struct field descriptors. Unlike the compatibility version, the native implementation includes one sqlvar_struct in the main structure, so it only needs to allocate additional space for (sqld - 1) extra field descriptors. The function also adds alignment padding to ensure proper alignment for the first data field.

The "empty" designation indicates this calculation is for structural metadata only, not including space for actual data values that would be stored separately.

## Parameters / Member Variables
- : Pointer to a PostgreSQL result set (PGresult) containing query results and metadata

## Dependencies
- Functions called/Symbols referenced:
  - [PQnfields](../P/PQnfields.md) (to get the number of fields in the result set)
  - [ecpg_sqlda_align_add_size](../e/ecpg_sqlda_align_add_size.md) (for alignment calculations)
  - [sqlda_struct](sqlda_struct.md) (native SQLDA structure type)
  - [sqlvar_struct](sqlvar_struct.md) (native field descriptor structure type)
- Called from (representative examples):
  - [sqlda_native_total_size](sqlda_native_total_size.md)
  - [ecpg_set_native_sqlda](../e/ecpg_set_native_sqlda.md)

## Notes and Other Information
This function is part of PostgreSQL's ECPG native-mode SQLDA implementation, which provides a more modern and efficient interface compared to the compatibility mode. The native mode structure differs from compatibility mode in its internal layout and field organization. The calculation accounts for the fact that the main sqlda_struct already includes space for one sqlvar_struct, so only additional field descriptors need to be allocated. This optimization reduces memory overhead compared to the compatibility mode. The alignment padding ensures optimal memory access patterns for subsequent data storage operations.