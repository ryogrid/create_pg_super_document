# DefineAggregate

## Location
[src/backend/commands/aggregatecmds.c:53-477](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/aggregatecmds.c#L53-L477)

## Overview
DefineAggregate is the top-level function responsible for parsing and processing the CREATE AGGREGATE command in PostgreSQL, handling all aspects of aggregate function definition validation and creation.

## Definition


## Detailed Description
DefineAggregate serves as the main entry point for CREATE AGGREGATE statement processing. It parses the aggregate definition from SQL command parameters, validates the aggregate specification including all required and optional functions (transition, final, combine, serial/deserial, moving-window variants), handles both old-style (pre-8.2) and new-style parameter formats, and performs extensive validation of aggregate parameters including type checking, permission verification, and consistency validation. The function supports various aggregate types including normal aggregates, ordered-set aggregates, and hypothetical-set aggregates, as well as moving-window aggregates with forward and inverse transition functions. After thorough validation, it delegates the actual aggregate creation to AggregateCreate.

## Parameters / Member Variables
- : Parse state containing parsing context and error information
- : Qualified name list specifying the aggregate name and optional schema
- : Function parameter list defining aggregate arguments (format depends on oldstyle flag)
- : Boolean indicating old-style syntax (pre-8.2) using BASETYPE parameter
- : List of DefElem nodes representing aggregate definition clauses (sfunc, stype, finalfunc, etc.)
- : Boolean indicating whether to replace existing aggregate (CREATE OR REPLACE)

## Dependencies
- Functions called/Symbols referenced:
  - [QualifiedNameGetCreationNamespace](../Q/QualifiedNameGetCreationNamespace.md)
  - [object_aclcheck](../o/object_aclcheck.md)
  - [extractModify](../e/extractModify.md)
  - [typenameTypeId](../t/typenameTypeId.md)
  - [interpret_function_parameter_list](../i/interpret_function_parameter_list.md)
  - [AggregateCreate](../A/AggregateCreate.md)
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md)

## Notes and Other Information
DefineAggregate handles complex validation logic for different aggregate types and parameter combinations. It supports both traditional aggregates and advanced features like moving-window aggregates, parallel execution modes, and serialization functions for custom aggregate state. The function enforces strict consistency rules between related parameters (e.g., moving-aggregate functions must be specified together) and provides detailed error messages for invalid configurations. The oldstyle parameter maintains backward compatibility with PostgreSQL versions prior to 8.2.