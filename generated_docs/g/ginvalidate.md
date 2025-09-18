# ginvalidate

## Location
[src/backend/access/gin/ginvalidate.c:31-276](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginvalidate.c#L31-L276)

## Overview
Validates a GIN (Generalized Inverted Index) operator class to ensure it contains all required support functions and operators with correct signatures and parameters.

## Definition


## Detailed Description
The  function performs comprehensive validation of a GIN operator class by checking:
1. **Support function validation**: Verifies that all required GIN support functions are present with correct signatures
2. **Operator validation**: Ensures operators have valid strategy numbers (1-63) and proper signatures
3. **Consistency checks**: Validates that operator/function groups are internally consistent
4. **Completeness verification**: Confirms the operator class contains all mandatory functions

The function validates the following GIN support functions:
-  (1): Key comparison function (optional)
-  (2): Extract keys from indexed values (required)
-  (3): Extract keys from query conditions (required)
-  (4): Test whether entry is consistent with query (required if no triconsistent)
-  (5): Compare partial-match query key (optional)
-  (6): Ternary consistency check (required if no consistent)
-  (7): Parse reloptions for index (optional)

## Parameters / Member Variables
- : OID of the operator class to validate

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - SearchSysCacheList1
  - [check_amproc_signature](../c/check_amproc_signature.md)
  - [check_amoptsproc_signature](../c/check_amoptsproc_signature.md)
  - [check_amop_signature](../c/check_amop_signature.md)
  - [identify_opfamily_groups](../i/identify_opfamily_groups.md)
  - [format_procedure](../f/format_procedure.md)
  - [format_operator](../f/format_operator.md)
  - [ReleaseCatCacheList](../R/ReleaseCatCacheList.md)
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
- Called from (representative examples):
  - [ginhandler](ginhandler.md) (src/backend/access/gin/ginutil.c:75)

## Notes and Other Information
- Returns  if the operator class is valid,  if validation errors are found
- Validation errors are reported using  calls
- GIN operator classes must have either GIN_CONSISTENT_PROC or GIN_TRICONSISTENT_PROC (or both)
- GIN does not support ORDER BY operators (amoppurpose must be AMOP_SEARCH)
- Strategy numbers for GIN operators must be between 1 and 63
- All support functions must have matching left/right input types
- The function performs thorough signature checking for each support function type