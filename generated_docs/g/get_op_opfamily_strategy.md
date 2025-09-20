# get_op_opfamily_strategy

## Location
[src/backend/utils/cache/lsyscache.c:83-107](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L83-L107)

## Overview
Retrieves the strategy number of an operator within a specified operator family, or returns 0 if the operator is not a member of that family.

## Definition

```c
int
get_op_opfamily_strategy(Oid opno, Oid opfamily)
```
## Detailed Description
This function looks up an operator in the pg_amop system catalog to determine its strategy number within a given operator family. Strategy numbers define the semantic meaning of operators within an operator family (e.g., 1 for less-than, 2 for less-equal, 3 for equal, etc.). The function only considers search operators (AMOP_SEARCH), not ordering operators. If the operator is not found in the specified family, it returns 0.

## Parameters / Member Variables
- : The OID of the operator to look up
- : The OID of the operator family to search within

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache3](../S/SearchSysCache3.md) (system cache lookup function)
  - HeapTupleIsValid (checks if tuple is valid)
  - GETSTRUCT (extracts structure from heap tuple)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (releases cache reference)
  - Form_pg_amop (structure type for pg_amop catalog)
  - AMOP_SEARCH (constant for search operator type)
  - [CharGetDatum](../C/CharGetDatum.md) (datum conversion function)
- Called from (representative examples):
  - [ComputeIndexAttrs](../C/ComputeIndexAttrs.md) (src/backend/commands/indexcmds.c:2103)
  - [match_rowcompare_to_indexcol](../m/match_rowcompare_to_indexcol.md) (src/backend/optimizer/path/indxpath.c:2757)
  - [expand_indexqual_rowcompare](../e/expand_indexqual_rowcompare.md) (src/backend/optimizer/path/indxpath.c:2880)
  - [get_actual_variable_range](get_actual_variable_range.md) (src/backend/utils/adt/selfuncs.c:6205)
  - [btcostestimate](../b/btcostestimate.md) (src/backend/utils/adt/selfuncs.c:6963)
  - [RelationGetExclusionInfo](../R/RelationGetExclusionInfo.md) (src/backend/utils/cache/relcache.c:5697)

## Notes and Other Information
- Strategy numbers are specific to each operator family and define the semantic meaning of operators
- Common strategy numbers include: 1 (less-than), 2 (less-equal), 3 (equal), 4 (greater-equal), 5 (greater-than)
- Returns 0 when the operator is not found in the family, which can be used as a boolean test
- Only searches for search operators (AMOP_SEARCH), excluding ordering operators (AMOP_ORDER)
- Uses proper cache management with SearchSysCache3/ReleaseSysCache pair
- The strategy number is retrieved from the amopstrategy field of the pg_amop catalog entry