# pg_get_partkeydef_worker

## Location
src/backend/utils/adt/ruleutils.c: 1917 - 2075

## Overview
Internal workhorse function that decompiles and reconstructs a partition key definition from the system catalogs, providing flexible output formatting options.

## Definition


## Detailed Description
This is the core implementation function for generating string representations of partition key definitions. It retrieves partition information from the  system catalog and reconstructs the partition clause syntax. The function handles different partition strategies (HASH, LIST, RANGE) and can format output in multiple modes - either as a complete "PARTITION BY" clause or just the column/expression list. It processes both simple column references and complex expressions, handling collations and operator classes appropriately.

## Parameters / Member Variables
- : Object identifier (OID) of the partitioned relation whose partition key definition should be retrieved
- : Integer flags controlling the formatting and pretty-printing of the output (derived from GET_PRETTY_FLAGS macro)
- : Boolean flag indicating whether to return only the column/expression list (true) or the full partition clause including strategy (false)
- : Boolean flag indicating whether to return NULL (true) or throw an error (false) if the relation is not found

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCache1, SysCacheGetAttrNotNull, ReleaseSysCache (system catalog access)
  - Form_pg_partitioned_table, oidvector (data structure types)
  - heap_attisnull, TextDatumGetCString, stringToNode (tuple and expression processing)
  - deparse_context_for, get_relation_name, deparse_expression_pretty (expression deparsing)
  - get_attname, quote_identifier, get_atttypetypmodcoll (attribute information)
  - generate_collation_name, get_opclass_name (formatting helpers)
  - looks_like_function, exprType, exprCollation (expression analysis)
- Called from (representative examples):
  - pg_get_partkeydef (public function for full partition definitions)
  - pg_get_partkeydef_columns (public function for column-only definitions)

## Notes and Other Information
- This is a static (internal) function not exposed in the public API
- Handles three partition strategies: HASH, LIST, and RANGE
- Properly formats both simple column references and complex expressions with parentheses as needed
- Manages collation specifications when they differ from the column's default collation
- Includes operator class names in the output when they are not the default for the data type
- Uses StringInfo for efficient string building
- Implements proper error handling for missing relations and malformed partition expressions
- The function reconstructs the original partition key definition from stored catalog data, not from cached parsed structures