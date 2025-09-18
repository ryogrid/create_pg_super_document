# resolve_polymorphic_tupdesc

## Location
src/backend/utils/fmgr/funcapi.c: 744 - 1063

## Overview
Resolves polymorphic column types in a function's result tuple descriptor by replacing them with concrete data types deduced from the function's input arguments.

## Definition
static bool resolve_polymorphic_tupdesc(TupleDesc tupdesc, oidvector *declared_args, Node *call_expr)

## Detailed Description
This function is the core polymorphic type resolution engine for function return types with OUT parameters. It processes a tuple descriptor containing polymorphic column types (ANYELEMENT, ANYARRAY, ANYRANGE, etc.) and replaces them with concrete types based on the actual types passed as input arguments.

The function handles both the traditional polymorphic type family (ANYELEMENT, ANYARRAY, ANYRANGE, ANYMULTIRANGE) and the newer ANYCOMPATIBLE family. It works in several phases:

1. **Detection Phase**: Scans the tuple descriptor to identify which polymorphic types are present in the output
2. **Extraction Phase**: Examines the input arguments to extract concrete types for each polymorphic type family
3. **Resolution Phase**: Uses the resolve_*_from_others() helper functions to deduce missing polymorphic types from known ones
4. **Collation Phase**: Determines appropriate collations for the resolved types
5. **Replacement Phase**: Updates the tuple descriptor with the concrete types and collations

The function supports type inference between related polymorphic types (e.g., deducing ANYARRAY from ANYELEMENT) and handles collation inheritance from input expressions.

## Parameters / Member Variables
- : The tuple descriptor containing polymorphic column types to be resolved
- : OID vector of the function's declared input argument types, indicating which are polymorphic
- : The function call expression containing actual argument types, or NULL if not available

## Dependencies
- Functions called/Symbols referenced:
  - get_call_expr_argtype: Extracts actual argument type from call expression
  - resolve_anyelement_from_others: Resolves ANYELEMENT from other polymorphic types
  - resolve_anyarray_from_others: Resolves ANYARRAY from other polymorphic types  
  - resolve_anyrange_from_others: Resolves ANYRANGE from other polymorphic types
  - resolve_anymultirange_from_others: Resolves ANYMULTIRANGE from other polymorphic types
  - get_typcollation: Gets collation for a data type
  - exprInputCollation: Determines input collation from expression
  - TupleDescInitEntry: Initializes tuple descriptor entry with resolved type
  - TupleDescInitEntryCollation: Sets collation for tuple descriptor entry

- Called from (representative examples):
  - internal_get_result_type: When determining result types for functions with OUT parameters

## Notes and Other Information
- This is a static function, only used within funcapi.c  
- Returns true if all polymorphic types could be resolved, false if insufficient information is available
- Handles both traditional polymorphic types (ANY*) and compatible polymorphic types (ANYCOMPATIBLE*)
- Collation handling differs between type families - range types don't use collations
- The function assumes the parser has already validated argument type consistency
- Located in src/backend/utils/fmgr/funcapi.c:744-1063