# infer_collation_opclass_match

## Location
[src/backend/optimizer/util/plancat.c:978-1059](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/plancat.c#L978-L1059)

## Overview
Verifies that inference specification elements match the collation and operator class requirements of indexed attributes during ON CONFLICT clause processing.

## Definition


## Detailed Description
This static function ensures that inference elements from ON CONFLICT clauses properly match the collation and operator class constraints of indexed attributes. When a user specifies collation or opclass in an inference specification, this function validates that at least one indexed attribute matches those requirements.

The function implements a forgiving matching strategy that tolerates redundancy within cataloged index attributes. It only performs validation when the inference element explicitly specifies collation or opclass - in the common case where neither is specified, it returns true immediately, allowing matches regardless of the cataloged collation/opclass.

The matching process iterates through all index attributes and checks:
1. Operator family and input type compatibility for specified opclasses
2. Collation compatibility for specified collations  
3. Expression or attribute equivalence

For expression-based attributes (attno == 0), it compares the inference element expression with the corresponding indexed expression using exact equality matching.

## Parameters / Member Variables
- : InferenceElem containing the inference specification with potential collation/opclass constraints
- : Open Relation structure representing the index being evaluated for compatibility
- : List of index expressions for expression-based index attributes

## Dependencies
- Functions called/Symbols referenced:
  - [get_opclass_family](../g/get_opclass_family.md)
  - [get_opclass_input_type](../g/get_opclass_input_type.md)
  - [list_nth](../l/list_nth.md)
  - [equal](../e/equal.md)
  - IsA macro
- Called from (representative examples):
  - [infer_arbiter_indexes](infer_arbiter_indexes.md)

## Notes and Other Information
- Returns true immediately if no collation/opclass constraints are specified
- Handles both plain attribute references (Var nodes) and expression-based index elements
- Does not consider RelabelType nodes (unlike match_index_to_operand)
- Implements forgiving redundancy handling across multiple indexed attributes
- Both opclass and collation must match simultaneously when both are specified
- Supports PostgreSQL's historical lack of alternative equality notions in collations/opclasses