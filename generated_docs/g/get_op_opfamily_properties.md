# get_op_opfamily_properties

## Location
[src/backend/utils/cache/lsyscache.c:136-165](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L136-L165)

## Overview
Retrieves comprehensive properties of an operator within a specified operator family, including strategy number and input data types for both operands.

## Definition


## Detailed Description
This function provides a comprehensive lookup of an operator's properties within an operator family. It retrieves the strategy number, left operand type, and right operand type from the pg_amop catalog. The function can handle both search operators and ordering operators based on the ordering_op parameter. Unlike other similar functions, this one assumes the operator is already known to be a member of the family and will raise an error if not found, making it suitable for cases where membership has been pre-verified.

## Parameters / Member Variables
- : The OID of the operator to look up
- : The OID of the operator family to search within
- : Boolean flag indicating whether to look for ordering operators (true) or search operators (false)
- : Output parameter - pointer to store the operator's strategy number
- : Output parameter - pointer to store the OID of the left operand type
- : Output parameter - pointer to store the OID of the right operand type

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache3](../S/SearchSysCache3.md) (system cache lookup function)
  - HeapTupleIsValid (checks if tuple is valid)
  - GETSTRUCT (extracts structure from heap tuple)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (releases cache reference)
  - elog (error logging function)
  - Form_pg_amop (structure type for pg_amop catalog)
  - AMOP_ORDER (constant for ordering operator type)
  - AMOP_SEARCH (constant for search operator type)
  - [CharGetDatum](../C/CharGetDatum.md) (datum conversion function)
- Called from (representative examples):
  - [ExecInitExprRec](../E/ExecInitExprRec.md) (src/backend/executor/execExpr.c:2055)
  - [ExecIndexBuildScanKeys](../E/ExecIndexBuildScanKeys.md) (src/backend/executor/nodeIndexscan.c:1224, 1342, 1465)
  - [MJExamineQuals](../M/MJExamineQuals.md) (src/backend/executor/nodeMergejoin.c:224)
  - [expand_indexqual_rowcompare](../e/expand_indexqual_rowcompare.md) (src/backend/optimizer/path/indxpath.c:2833, 2894)
  - [gen_prune_steps_from_opexps](gen_prune_steps_from_opexps.md) (src/backend/partitioning/partprune.c:1425)

## Notes and Other Information
- This function assumes the operator is already known to be in the family and will ERROR if not found
- Callers should verify membership using op_in_opfamily() before calling this function if unsure
- The function can handle both search and ordering operators via the ordering_op parameter
- All three output parameters (strategy, lefttype, righttype) are mandatory and must be valid pointers
- Strategy numbers and data types are essential for proper operator resolution and type checking
- Commonly used during query execution planning and index scan key building
- The data types returned represent the declared input types for the operator, which may differ from the actual runtime types due to implicit casting