# transformRangeTableFunc

## Location
src/backend/parser/parse_clause.c: 688 - 909

## Overview
Transforms a raw RangeTableFunc (currently XMLTABLE construct) into a TableFunc structure, processing namespace clauses, document expression, row expression, column specifications, and default values.

## Definition
static ParseNamespaceItem *
transformRangeTableFunc(ParseState *pstate, RangeTableFunc *rtf)

## Detailed Description
The transformRangeTableFunc function handles the transformation of table function constructs, currently specifically supporting XMLTABLE functionality. The function creates a TableFunc structure and processes all components: it transforms and type-coerces the row-generating and document-generating expressions to appropriate types (TEXT and XML respectively), processes column specifications including FOR ORDINALITY columns, handles namespace declarations with validation for uniqueness and single default namespace, and manages type information including collations. The function also enables lateral references and determines whether the RTE should be marked as LATERAL based on cross-references or explicit specification.

## Parameters / Member Variables
- pstate: ParseState structure containing the current parsing context and state information  
- rtf: RangeTableFunc structure representing the raw table function construct to be transformed, including expressions, column definitions, namespaces, and lateral flag

## Dependencies
- Functions called/Symbols referenced:
  - makeNode
  - [transformExpr](transformExpr.md)
  - [coerce_to_specific_type](../c/coerce_to_specific_type.md)
  - [coerce_to_specific_type_typmod](../c/coerce_to_specific_type_typmod.md)
  - [assign_expr_collations](../a/assign_expr_collations.md)
  - [typenameTypeIdAndMod](typenameTypeIdAndMod.md)
  - [get_typcollation](../g/get_typcollation.md)
  - [bms_add_member](../b/bms_add_member.md)
  - [contain_vars_of_level](../c/contain_vars_of_level.md)
  - [addRangeTableEntryForTableFunc](../a/addRangeTableEntryForTableFunc.md)
  - TFT_XMLTABLE
  - EXPR_KIND_FROM_FUNCTION
- Called from (representative examples):
  - [transformFromClauseItem](transformFromClauseItem.md)

## Notes and Other Information
- Currently only supports XMLTABLE functionality (TFT_XMLTABLE), with JSON_TABLE support handled elsewhere
- Automatically enables lateral references (p_lateral_active = true) for SQL spec compliance
- Row expression is coerced to TEXT type, document expression to XML type  
- FOR ORDINALITY columns are automatically assigned INT4OID type with typmod -1
- Only one FOR ORDINALITY column is allowed per table function
- Column names must be unique within the table function
- Namespace processing validates uniqueness of named namespaces and allows only one default namespace
- Default namespace is represented internally as NULL pointer
- SETOF types are not allowed for individual columns
- Column expressions (PATH) are coerced to TEXT, default expressions to the target column type
- The function maintains NOT NULL information using a bitmap (tf->notnulls)
- Type collations are automatically assigned for all expressions and columns