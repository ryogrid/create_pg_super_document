# ComputePartitionAttrs

## Location
[src/backend/commands/tablecmds.c:18046-18303](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L18046-L18303)

## Overview
Computes per-partition-column metadata including attribute numbers, operator classes, and collations from parsed PartitionElem specifications, while validating partition key constraints and expression requirements.

## Definition
```c
static void ComputePartitionAttrs(ParseState *pstate, Relation rel, List *partParams, 
                                  AttrNumber *partattrs, List **partexprs, Oid *partopclass, 
                                  Oid *partcollation, PartitionStrategy strategy)
```

## Detailed Description
This comprehensive function processes a list of PartitionElem structures to extract and validate all necessary metadata for partition key columns. It handles both simple column references and complex expressions, performing extensive validation including type compatibility, mutability constraints, collation requirements, and operator class resolution.

For simple column references, it validates column existence, prohibits system columns and generated columns, and extracts type information from the relation descriptor. For expressions, it performs additional validation including immutability checks, constant expression rejection, system column reference detection, and proper type validation for partition keys.

The function determines appropriate operator classes based on partitioning strategy (btree for LIST/RANGE, hash for HASH) and resolves collation requirements for collatable data types.

## Parameters / Member Variables
- `pstate`: ParseState for error reporting and context information
- `rel`: Target relation being partitioned
- `partParams`: List of PartitionElem structures containing partition key specifications
- `partattrs`: Output array to store attribute numbers (0 for expressions)
- `partexprs`: Output list to store partition expressions (NULL for simple columns)
- `partopclass`: Output array to store operator class OIDs for each partition key
- `partcollation`: Output array to store collation OIDs for each partition key  
- `strategy`: Partitioning strategy (HASH, LIST, RANGE) affecting operator class selection

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCacheAttName](../S/SearchSysCacheAttName.md) - Looks up column information by name
  - exprType, exprCollation - Extract expression type and collation
  - [CheckAttributeType](CheckAttributeType.md) - Validates type suitability for partition keys
  - [pull_varattnos](../p/pull_varattnos.md) - Extracts attribute references from expressions
  - [expression_planner](../e/expression_planner.md) - Preprocesses expressions for mutability analysis
  - [contain_mutable_functions](../c/contain_mutable_functions.md) - Checks for non-immutable functions
  - [get_collation_oid](../g/get_collation_oid.md) - Resolves collation names to OIDs
  - [type_is_collatable](../t/type_is_collatable.md) - Determines if type supports collation
  - [GetDefaultOpClass](../G/GetDefaultOpClass.md), ResolveOpClass - Operator class resolution
- Called from (representative examples):
  - [DefineRelation](../D/DefineRelation.md) (src/backend/commands/tablecmds.c:1172)

## Notes and Other Information
- Static function scope limits visibility to tablecmds.c module
- Enforces strict immutability requirements for partition expressions to ensure consistent routing
- Prohibits system columns, generated columns, and constant expressions in partition keys
- Strips top-level COLLATE clauses from expressions for consistent handling
- Automatically selects appropriate access method (btree vs hash) based on partitioning strategy
- Provides detailed error messages with location information for debugging
- Essential for partition constraint generation and tuple routing functionality
- Part of the table command infrastructure in src/backend/commands/tablecmds.c (lines 18046-18303)