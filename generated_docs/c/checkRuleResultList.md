# checkRuleResultList

## Location
src/backend/rewrite/rewriteDefine.c: 506 - 630

## Overview
checkRuleResultList validates that a target list (either SELECT or RETURNING) produces output that is compatible with a relation's tuple descriptor, ensuring type and structure consistency.

## Definition


## Detailed Description
checkRuleResultList performs comprehensive validation of target lists against relation schemas to ensure compatibility when creating rules. It validates that the number of entries matches the relation's attribute count, verifies type compatibility between target list expressions and corresponding relation columns, checks column name matching when required (for SELECT rules), and handles type modifier (typmod) validation with appropriate flexibility for unspecified cases. The function also prevents operations on relations with dropped columns, as supporting them would require significant infrastructure changes. It provides detailed error messages distinguishing between SELECT target lists and RETURNING lists for better user feedback.

## Parameters / Member Variables
- : List of TargetEntry nodes representing the output columns to validate
- : TupleDesc describing the expected schema of the target relation
- : Boolean flag indicating whether this is a SELECT target list (vs RETURNING list) for error message context
- : Boolean requiring exact column name matching (only valid when isSelect is true)

## Dependencies
- Functions called/Symbols referenced:
  - [TargetEntry](../T/TargetEntry.md) (struct access)
  - TupleDescAttr
  - NameStr
  - exprType
  - exprTypmod
  - [format_type_be](../f/format_type_be.md)
  - [format_type_with_typemod](../f/format_type_with_typemod.md)
- Called from (representative examples):
  - [DefineQueryRewrite](../D/DefineQueryRewrite.md) (twice - for SELECT rules and RETURNING validation)

## Notes and Other Information
- This is a static function internal to rewriteDefine.c used specifically for rule validation
- Ignores resjunk (junk result) entries in the target list as they don't correspond to output columns
- Enforces strict type matching but allows typmod differences when one is unspecified (-1)
- Prevents creation of rules on relations with dropped columns due to implementation complexity
- Provides context-sensitive error messages that distinguish between SELECT rules and RETURNING lists
- Critical for maintaining data integrity when creating view rules and RETURNING clause validation
- The requireColumnNameMatch parameter is only used for SELECT rules on views where exact column name correspondence is required