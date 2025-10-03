# op_in_opfamily

## Location
[src/backend/utils/cache/lsyscache.c:66-82](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L66-L82)

## Overview
Checks whether a given operator is a member of a specified operator family, considering only search operators (not ordering operators).

## Definition

```c
bool
op_in_opfamily(Oid opno, Oid opfamily)
```
## Detailed Description
This function provides a simple boolean check to determine if an operator identified by  belongs to the operator family identified by . It specifically looks for search operators (AMOP_SEARCH) in the system catalog cache, excluding ordering operators. The function uses the system cache lookup mechanism to efficiently check the pg_amop catalog for the existence of the specified operator-family relationship.

## Parameters / Member Variables
- `opno`: The OID of the operator to check
- `opfamily`: The OID of the operator family to search within
## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCacheExists3 (system cache lookup function)
  - AMOP_SEARCH (constant for search operator type)
  - [CharGetDatum](../C/CharGetDatum.md) (datum conversion function)
- Called from (representative examples):
  - [IsBooleanOpfamily](../I/IsBooleanOpfamily.md) (src/backend/optimizer/path/indxpath.c:2285)
  - [match_opclause_to_indexcol](../m/match_opclause_to_indexcol.md) (src/backend/optimizer/path/indxpath.c:2438, 2471)
  - [match_saopclause_to_indexcol](../m/match_saopclause_to_indexcol.md) (src/backend/optimizer/path/indxpath.c:2659)
  - [relation_has_unique_index_for](../r/relation_has_unique_index_for.md) (src/backend/optimizer/path/indxpath.c:3572)
  - [equality_ops_are_compatible](../e/equality_ops_are_compatible.md) (src/backend/utils/cache/lsyscache.c:723)
  - [comparison_ops_are_compatible](../c/comparison_ops_are_compatible.md) (src/backend/utils/cache/lsyscache.c:772)

## Notes and Other Information
- The function only considers search operators, which are used for equality and comparison operations in index scans
- It does not check for ordering operators (AMOP_ORDER), which are used for sorting operations
- Uses the AMOPOPID cache which indexes the pg_amop catalog by operator OID, strategy, and operator family OID
- This is a lightweight cache lookup operation that avoids direct catalog table access
- Commonly used in query optimization to determine if operators can be used with specific index access methods

## Simplified Source

```c
bool op_in_opfamily(Oid opno, Oid opfamily) {
    // Check if operator exists in the given operator family
    // Only considers search operators (AMOP_SEARCH), not ordering operators
    return SearchSysCacheExists3(AMOPOPID,
                                ObjectIdGetDatum(opno),
                                CharGetDatum(AMOP_SEARCH),
                                ObjectIdGetDatum(opfamily));
}
```