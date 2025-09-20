# RangeVar

## Location
[src/include/nodes/primnodes.h:71-95](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/primnodes.h#L71-L95)

## Overview
RangeVar represents a range variable used in FROM clauses and utility statements, providing a structured way to reference relations with optional catalog, schema, and alias information.

## Definition

```c
typedef struct RangeVar
{
	NodeTag		type;

	/* the catalog (database) name, or NULL */
	char	   *catalogname;

	/* the schema name, or NULL */
	char	   *schemaname;

	/* the relation/sequence name */
	char	   *relname;

	/* expand rel by inheritance? recursively act on children? */
	bool		inh;

	/* see RELPERSISTENCE_* in pg_class.h */
	char		relpersistence;

	/* table alias & optional column aliases */
	Alias	   *alias;

	/* token location, or -1 if unknown */
	ParseLoc	location;
} RangeVar;
```
## Detailed Description
RangeVar is a fundamental structure in PostgreSQL's parser that represents table and relation references throughout the system. It serves as a comprehensive container for relation identification, supporting fully qualified names (catalog.schema.relation), inheritance behavior control, and aliasing capabilities.

In FROM clauses, RangeVar provides the foundation for relation references with optional aliasing. In utility statements (DDL operations), the alias field is typically unused, but the inheritance flag (inh) controls whether operations should be applied recursively to child tables in inheritance hierarchies.

The structure supports temporary table indicators through the relpersistence field and maintains parse location information for error reporting and debugging purposes.

## Parameters / Member Variables
- : Standard NodeTag for PostgreSQL's node system type identification
- : Database name (catalog) for cross-database references, or NULL for current database
- : Schema name for qualified references, or NULL for search path resolution
- : The actual relation/sequence name (required field)
- : Boolean flag controlling inheritance expansion and recursive operations on child tables
- : Character indicating relation persistence (TEMP, UNLOGGED, PERMANENT - see RELPERSISTENCE_* constants)
- : Pointer to Alias struct for table and column aliasing, or NULL if no alias
- : Parse location in the original query string for error reporting, or -1 if unknown

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (node system)
  - [Alias](../A/Alias.md) (for aliasing support)
  - ParseLoc (parse location tracking)
  - RELPERSISTENCE_* constants

- Called from (representative examples):
  - [RangeVarGetRelidExtended](RangeVarGetRelidExtended.md) (relation resolution)
  - [parserOpenTable](../p/parserOpenTable.md) (table access)
  - [addRangeTableEntry](../a/addRangeTableEntry.md) (RTE creation)
  - [transformFromClauseItem](../t/transformFromClauseItem.md) (FROM clause parsing)
  - [makeRangeVar](../m/makeRangeVar.md) (creation function)
  - [DefineRelation](../D/DefineRelation.md) (table creation)
  - various utility commands (DROP, ALTER, etc.)

## Notes and Other Information
- Central to PostgreSQL's table/relation reference system
- Used extensively in both SELECT queries and utility commands
- Supports cross-database references via catalogname (though rarely used)
- The inh flag is crucial for inheritance hierarchy operations
- Parse location tracking enables precise error reporting
- Can represent temporary, unlogged, or permanent relations via relpersistence
- Forms the basis for Range Table Entry (RTE) creation in the planner
- Essential for namespace resolution and relation lookup operations