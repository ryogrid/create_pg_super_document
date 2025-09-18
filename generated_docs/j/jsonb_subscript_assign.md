# jsonb_subscript_assign

## Location
[src/backend/utils/adt/jsonbsubs.c:261-322](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonbsubs.c#L261-L322)

## Overview
Evaluates a SubscriptingRef assignment operation to set an element within a JSONB container, creating the container if it doesn't exist.

## Definition


## Detailed Description
This function performs JSONB element assignment operations during expression evaluation. It handles setting values within existing JSONB containers or creating new containers when the source is NULL. The function implements smart container creation logic based on the subscript types - creating arrays when the first subscript is an integer, and objects otherwise.

Key behaviors include:
1. **Replacement Value Processing**: Converts the replacement value from Datum to JsonbValue format, handling NULL values appropriately
2. **NULL Source Handling**: When the source container is NULL, creates an empty array (if first subscript is integer) or empty object (otherwise)
3. **Assignment Delegation**: Uses jsonb_set_element to perform the actual assignment operation
4. **Result Management**: Ensures the result is never NULL after assignment

The function leverages the workspace expectArray flag (set during subscript checking) to determine the appropriate container type when creating new JSONB values.

## Parameters / Member Variables
- : Expression evaluation state (not directly used in this function)
- : Expression evaluation step containing the SubscriptingRefState and result storage locations
- : Expression context for evaluation (not directly used in this function)

## Dependencies
- Functions called/Symbols referenced:
  - [JsonbToJsonbValue](../J/JsonbToJsonbValue.md)
  - [DatumGetJsonbP](../D/DatumGetJsonbP.md)
  - [JsonbValueToJsonb](../J/JsonbValueToJsonb.md)
  - [jsonb_set_element](jsonb_set_element.md)
- Called from:
  - [jsonb_exec_setup](jsonb_exec_setup.md)

## Notes and Other Information
- Creates empty arrays when expectArray is true (first subscript is integer), otherwise creates empty objects
- The replacement value is taken from sbsrefstate->replacevalue/replacenull
- Handles NULL replacement values by setting JsonbValue type to jbvNull
- The result is guaranteed to be non-NULL after assignment, so op->resnull is not modified
- Uses the workspace index array populated by jsonb_subscript_check_subscripts
- Delegates actual assignment logic to jsonb_set_element function
- Part of the expression evaluation framework for JSONB subscripting assignment operations