# _readRangeTblEntry

## Location
src/backend/nodes/readfuncs.c: 347 - 438

## Overview
A static function that deserializes a RangeTblEntry node from its string representation, handling all types of range table entries used in PostgreSQL query planning.

## Definition


## Detailed Description
The  function reconstructs RangeTblEntry nodes from their serialized format during PostgreSQL's node deserialization process. A RangeTblEntry represents a single item in the FROM clause of a SQL query and can take various forms depending on the type of data source.

The function uses a switch statement based on the  field to handle different types of range table entries:
- **RTE_RELATION**: Regular tables or views
- **RTE_SUBQUERY**: Subqueries in the FROM clause
- **RTE_JOIN**: JOIN operations
- **RTE_FUNCTION**: Functions in the FROM clause
- **RTE_TABLEFUNC**: Table functions (like JSON_TABLE)
- **RTE_VALUES**: VALUES clauses
- **RTE_CTE**: Common Table Expressions (WITH clauses)
- **RTE_NAMEDTUPLESTORE**: Named tuple stores (for ephemeral relations)
- **RTE_RESULT**: Result relations

Each RTE type has its own specific fields that need to be deserialized. The function also handles special cases like copying column type information from TableFunc nodes and reusing certain RELATION fields for SUBQUERY and NAMEDTUPLESTORE types.

## Parameters / Member Variables
This function takes no parameters and returns a pointer to a newly allocated RangeTblEntry node.

## Dependencies
- Functions called/Symbols referenced:
  - READ_LOCALS (macro for local variable setup)
  - READ_NODE_FIELD (macro to read node fields)
  - READ_ENUM_FIELD (macro to read enum fields)
  - READ_OID_FIELD (macro to read OID fields)
  - READ_BOOL_FIELD (macro to read boolean fields)
  - READ_CHAR_FIELD (macro to read character fields)
  - READ_INT_FIELD (macro to read integer fields)
  - READ_UINT_FIELD (macro to read unsigned integer fields)
  - READ_STRING_FIELD (macro to read string fields)
  - READ_FLOAT_FIELD (macro to read float fields)
  - READ_DONE (macro for cleanup)
  - elog (error logging function)
- Called from (representative examples):
  - No direct references found (likely called via function pointer table)

## Notes and Other Information
- This is a static function, accessible only within readfuncs.c
- Handles all nine different types of range table entries with specialized field reading for each
- Implements special logic for RTE_TABLEFUNC to copy column type information from the TableFunc node
- Some RTE types (SUBQUERY, NAMEDTUPLESTORE) reuse fields originally designed for RELATION entries
- RTE_RESULT entries have no extra fields beyond the common ones
- Uses PostgreSQL's standard READ_* macro pattern for consistent field deserialization
- Part of the broader query plan serialization/deserialization system used for prepared statements and parallel query execution