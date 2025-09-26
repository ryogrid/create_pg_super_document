# JsonReturning

## Location
src/include/nodes/primnodes.h: 1660 - 1666

## Overview
JsonReturning represents the transformed representation of a JSON RETURNING clause, used to specify the output format and type information for JSON operations.

## Definition


## Detailed Description
JsonReturning is a node structure that encapsulates the specifications for how JSON results should be returned from JSON operations. It serves as the internal representation of SQL JSON RETURNING clauses, containing information about the desired output format and the target data type for the result.

This structure is created during the parsing and transformation phase when processing JSON expressions that include RETURNING clauses, and is used throughout the execution pipeline to ensure proper formatting and type conversion of JSON results.

## Parameters / Member Variables
- : Standard NodeTag for node type identification
- : Pointer to JsonFormat structure specifying the output JSON format details
- : OID of the target PostgreSQL data type for the returned value
- : Type modifier providing additional type-specific information (e.g., precision, length)

## Dependencies
- Functions called/Symbols referenced:
  - JsonFormat

- Called from (representative examples):
  - transformJsonOutput
  - transformJsonConstructorOutput
  - coerceJsonFuncExpr
  - makeJsonConstructorExpr
  - transformJsonObjectConstructor
  - transformJsonAggConstructor
  - transformJsonObjectAgg
  - transformJsonArrayAgg
  - transformJsonArrayConstructor
  - transformJsonIsPredicate
  - transformJsonReturning
  - transformJsonParseExpr
  - transformJsonScalarExpr
  - transformJsonSerializeExpr
  - transformJsonFuncExpr
  - transformJsonBehavior

## Notes and Other Information
- This structure is part of the PostgreSQL node system and follows standard node conventions
- It's primarily used in JSON expression processing and execution contexts
- The structure bridges the gap between SQL-level JSON RETURNING syntax and internal execution requirements
- Located in src/include/nodes/primnodes.h:1660-1666