# make_ruledef

## Location
src/backend/utils/adt/ruleutils.c: 5160 - 5351

## Overview
A static function that reconstructs the CREATE RULE command text for a given pg_rewrite tuple.

## Definition


## Detailed Description
The  function is responsible for reconstructing the complete CREATE RULE command from a PostgreSQL rule tuple stored in the pg_rewrite system catalog. This function extracts all rule attributes from the tuple and formats them into the standard SQL CREATE RULE syntax. It handles different event types (SELECT, UPDATE, INSERT, DELETE), optional qualifications (WHERE clauses), and rule actions. The function also handles formatting options and produces properly qualified object names.

The function processes rule metadata including the rule name, event type, target relation, qualification expressions, and action queries. It formats the output according to the specified pretty-printing flags and ensures proper quoting and qualification of identifiers.

## Parameters / Member Variables
- : StringInfo buffer where the reconstructed CREATE RULE command will be written
- : HeapTuple containing the rule data from pg_rewrite catalog
- : TupleDesc describing the structure of the rule tuple
- : Integer flags controlling output formatting (indentation, schema qualification, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - [SPI_fnumber](../S/SPI_fnumber.md) (gets attribute number by name)
  - SPI_getbinval (extracts binary attribute value)
  - [SPI_getvalue](../S/SPI_getvalue.md) (extracts string attribute value)
  - [DatumGetName](../D/DatumGetName.md), DatumGetChar, DatumGetObjectId, DatumGetBool (datum conversion functions)
  - [stringToNode](../s/stringToNode.md) (parses stored node trees)
  - [quote_identifier](../q/quote_identifier.md) (quotes SQL identifiers)
  - generate_relation_name, generate_qualified_relation_name (formats relation names)
  - [getInsertSelectQuery](../g/getInsertSelectQuery.md) (handles INSERT...SELECT rules)
  - [AcquireRewriteLocks](../A/AcquireRewriteLocks.md) (acquires necessary locks)
  - [set_deparse_for_query](../s/set_deparse_for_query.md) (sets up deparse context)
  - get_rule_expr (deparses qualification expressions)
  - [get_query_def](../g/get_query_def.md) (deparses action queries)
- Called from (representative examples):
  - [pg_get_ruledef_worker](../p/pg_get_ruledef_worker.md)

## Notes and Other Information
- This is a static function within ruleutils.c for internal use within the rule deparsing subsystem
- Handles all four PostgreSQL rule event types: SELECT (views), UPDATE, INSERT, and DELETE
- Properly manages deparse context for handling OLD and NEW variable references in rule qualifications
- Uses table locking (AccessShareLock) when accessing the target relation
- Supports both simple single-action rules and complex multi-action rules with parentheses
- Implements proper pretty-printing with configurable indentation and schema qualification
- Critical component of PostgreSQL's rule system introspection functionality
- Part of the pg_get_ruledef() SQL function implementation