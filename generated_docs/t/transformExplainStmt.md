# transformExplainStmt

## Location
src/backend/parser/analyze.c: 2961 - 3012

## Overview
Transforms an EXPLAIN statement into a CMD_UTILITY Query node after transforming the contained query and handling generic plan parameters.

## Definition


## Detailed Description
This function processes an EXPLAIN statement by first handling parameter setup for generic plans (if the GENERIC_PLAN option is specified), then transforming the contained query, and finally wrapping the result as a utility statement. Like other utility statement transformers, it performs transformation during parse analysis to ensure parser hooks execute at the expected time.

The function has special handling for generic plans: when no external parameter definitions exist and the GENERIC_PLAN option is specified, it sets up variable parameter definitions similar to how PREPARE statements work. This allows the EXPLAIN to work with parameterized queries even when parameter types aren't predetermined.

The transformation allows SELECT INTO statements within the explained query, unlike some other utility statement transformers.

## Parameters / Member Variables
- : Parse state containing context information for the transformation
- : The EXPLAIN statement to transform, containing:
  - : List of EXPLAIN options (ANALYZE, VERBOSE, BUFFERS, GENERIC_PLAN, etc.)
  - : The statement to be explained

## Dependencies
- Functions called/Symbols referenced:
  - makeNode, lfirst, strcmp
  - defGetBoolean, setup_parse_variable_parameters
  - transformOptionalSelectInto, check_variable_parameters
- Types referenced:
  - DefElem, ListCell, Oid
- Constants referenced:
  - CMD_UTILITY
- Called from (representative examples):
  - transformStmt

## Notes and Other Information
- Supports the GENERIC_PLAN option for parameter type inference when no external parameter source exists
- Unlike DECLARE CURSOR, EXPLAIN allows SELECT INTO in the contained query
- Parameter validation occurs after query transformation when using generic plans
- The function iterates through all GENERIC_PLAN options to use the last specified value
- Transformation occurs during parse analysis rather than execution to ensure proper parser hook behavior
- Returns a CMD_UTILITY query that will be executed by the utility statement execution framework