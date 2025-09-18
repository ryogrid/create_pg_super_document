# generateClonedExtStatsStmt

## Location
src/backend/parser/parse_utilcmd.c: 1865 - 1991

## Overview
Generates a CreateStatsStmt node by cloning the structure and properties of an existing extended statistics object, adjusting attribute numbers according to a provided mapping for use in table creation scenarios.

## Definition
static CreateStatsStmt *generateClonedExtStatsStmt(RangeVar *heapRel, Oid heapRelid, Oid source_statsid, const AttrMap *attmap)

## Detailed Description
This function creates a complete CreateStatsStmt that recreates an existing extended statistics object on a different table. It extracts all properties from the source statistics including the statistics types (ndistinct, dependencies, mcv), column references, and expression definitions. The function handles both simple column references and complex expression-based statistics, adjusting all attribute numbers using the provided attribute map. It processes the stxkind array to determine which types of extended statistics are enabled and builds appropriate StatsElem nodes for both columns and expressions. The resulting CreateStatsStmt can be executed to create equivalent extended statistics on the target table.

## Detailed Description
This function creates a complete CreateStatsStmt that recreates an existing extended statistics object on a different table. It extracts all properties from the source statistics including the statistics types (ndistinct, dependencies, mcv), column references, and expression definitions. The function handles both simple column references and complex expression-based statistics, adjusting all attribute numbers using the provided attribute map. It processes the stxkind array to determine which types of extended statistics are enabled and builds appropriate StatsElem nodes for both columns and expressions. The resulting CreateStatsStmt can be executed to create equivalent extended statistics on the target table.

## Parameters / Member Variables
- `heapRel`: RangeVar specifying the target table for the new extended statistics
- `heapRelid`: OID of the target table relation
- `source_statsid`: OID of the existing extended statistics object to clone
- `attmap`: AttrMap for translating attribute numbers from source to target table

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCache1
  - SysCacheGetAttrNotNull
  - SysCacheGetAttr
  - DatumGetArrayTypeP
  - TextDatumGetCString
  - stringToNode
  - makeString
  - makeNode
  - get_attname
  - map_variable_attnos
  - list_make1
  - ReleaseSysCache
- Called from (representative examples):
  - expandTableLikeClause

## Notes and Other Information
- Processes four types of extended statistics: ndistinct, dependencies, mcv, and expressions
- Expression statistics (STATS_EXT_EXPRESSIONS) are not exposed to users and are skipped
- Handles both simple column references and complex expressions in statistics definitions
- Expression order relative to attributes doesn't matter for extended statistics functionality
- Sets transformed=true to skip transformStatsStmt processing
- Does not clone the statistics name, allowing the system to generate a new one
- Uses the stxkind array to determine which statistics types are enabled on the source object
- Maps variable attribute numbers in expressions to match the target table structure