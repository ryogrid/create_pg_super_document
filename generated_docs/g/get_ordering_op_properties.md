# get_ordering_op_properties

## Location
[src/backend/utils/cache/lsyscache.c:207-266](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L207-L266)

## Overview
Determines the properties of a btree ordering operator, including its opfamily, input datatype, and strategy number.

## Definition

```c
bool
get_ordering_op_properties(Oid opno,
						   Oid *opfamily, Oid *opcintype, int16 *strategy)
```
## Detailed Description
This function takes an OID of an ordering operator (a btree "<" or ">" operator) and extracts key properties needed for query planning and optimization. It searches the pg_amop system catalog to find the operator's registration details within btree operator families.

The function ensures deterministic results when an operator is registered in multiple families by selecting the opfamily with the smallest OID. This prevents planning ambiguities that could arise from uncertain NULLS FIRST/LAST behavior or inefficient pathkey matching.

The function only considers btree access method operators with BTLessStrategyNumber or BTGreaterStrategyNumber strategies, and requires consistent left and right input types for the operator.

## Parameters / Member Variables
- : The OID of the ordering operator to analyze
- : Output parameter for the operator family OID 
- : Output parameter for the operator's declared input datatype
- : Output parameter for the strategy number (BTLessStrategyNumber or BTGreaterStrategyNumber)

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCacheList1
  - ReleaseSysCacheList
  - BTLessStrategyNumber
  - BTGreaterStrategyNumber
  - Form_pg_amop
  - [CatCList](../C/CatCList.md)
- Called from (representative examples):
  - [make_pathkey_from_sortop](../m/make_pathkey_from_sortop.md)
  - [build_expression_pathkey](../b/build_expression_pathkey.md)
  - [get_relation_info](get_relation_info.md)
  - [transformWindowDefinitions](../t/transformWindowDefinitions.md)
  - [get_equality_op_for_ordering_op](get_equality_op_for_ordering_op.md)
  - [PrepareSortSupportFromOrderingOp](../P/PrepareSortSupportFromOrderingOp.md)

## Notes and Other Information
- Returns true if successful, false if no matching pg_amop entry exists
- Output parameters are initialized to invalid values on failure (InvalidOid for OIDs, 0 for strategy)
- The function prioritizes deterministic results by choosing the opfamily with smallest OID when multiple registrations exist
- Only considers operators registered for the btree access method
- Requires operators to have consistent left and right input types