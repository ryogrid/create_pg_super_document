# get_opfamily_member

## Location
[src/backend/utils/cache/lsyscache.c:166-206](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L166-L206)

## Overview
Retrieves the OID of the operator that implements a specified strategy with specified data types within an operator family.

## Definition


## Detailed Description
This function performs a reverse lookup in the pg_amop catalog to find the specific operator that implements a given strategy number for particular data types within an operator family. Unlike the previous functions that start with a known operator, this function starts with the desired strategy and data types to find the appropriate operator. It uses the AMOPSTRATEGY cache which indexes by operator family, left type, right type, and strategy number. This is essential for query planning when the system needs to find operators that can handle specific data type combinations with desired semantics.

## Parameters / Member Variables
- : The OID of the operator family to search within
- : The OID of the left operand data type
- : The OID of the right operand data type  
- : The strategy number of the desired operator (typically 1-5 for comparison operators)

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache4](../S/SearchSysCache4.md) (system cache lookup function with 4 keys)
  - HeapTupleIsValid (checks if tuple is valid)
  - GETSTRUCT (extracts structure from heap tuple)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (releases cache reference)
  - Form_pg_amop (structure type for pg_amop catalog)
  - Int16GetDatum (datum conversion function for int16)
  - InvalidOid (constant representing invalid OID)
- Called from (representative examples):
  - [_bt_find_extreme_element](../b/_bt_find_extreme_element.md) (src/backend/access/nbtree/nbtutils.c:810)
  - [DefineIndex](../D/DefineIndex.md) (src/backend/commands/indexcmds.c:987, 1044)
  - [ATAddForeignKeyConstraint](../A/ATAddForeignKeyConstraint.md) (src/backend/commands/tablecmds.c:9841, 9854, 9859)
  - [select_equality_operator](../s/select_equality_operator.md) (src/backend/optimizer/path/equivclass.c:1781)
  - [create_indexscan_plan](../c/create_indexscan_plan.md) (src/backend/optimizer/plan/createplan.c:3139)
  - [mergejoinscansel](../m/mergejoinscansel.md) (src/backend/utils/adt/selfuncs.c:3025-3106)
  - [lookup_type_cache](../l/lookup_type_cache.md) (src/backend/utils/cache/typcache.c:556-736)

## Notes and Other Information
- Returns InvalidOid when no operator matches the specified criteria
- Uses SearchSysCache4 with the AMOPSTRATEGY cache for efficient lookup by all four key fields
- Essential for operator resolution during query planning and execution
- The returned operator OID can be used to invoke the actual operator function
- Strategy numbers typically follow standard conventions: 1 (less-than), 2 (less-equal), 3 (equal), 4 (greater-equal), 5 (greater-than)
- Data type OIDs must exactly match - the function does not perform type coercion
- Heavily used in index operations, join planning, and constraint enforcement
- The function only searches search operators (AMOP_SEARCH), not ordering operators