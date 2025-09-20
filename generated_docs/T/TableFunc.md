# TableFunc

## Location
[src/include/nodes/primnodes.h:109-146](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/primnodes.h#L109-L146)

## Overview
TableFunc represents table functions such as XMLTABLE and JSON_TABLE, providing a structured way to extract tabular data from structured documents with column definitions and filtering capabilities.

## Definition

```c
typedef struct TableFunc
{
	NodeTag		type;
	/* XMLTABLE or JSON_TABLE */
	TableFuncType functype;
	/* list of namespace URI expressions */
	List	   *ns_uris pg_node_attr(query_jumble_ignore);
	/* list of namespace names or NULL */
	List	   *ns_names pg_node_attr(query_jumble_ignore);
	/* input document expression */
	Node	   *docexpr;
	/* row filter expression */
	Node	   *rowexpr;
	/* column names (list of String) */
	List	   *colnames pg_node_attr(query_jumble_ignore);
	/* OID list of column type OIDs */
	List	   *coltypes pg_node_attr(query_jumble_ignore);
	/* integer list of column typmods */
	List	   *coltypmods pg_node_attr(query_jumble_ignore);
	/* OID list of column collation OIDs */
	List	   *colcollations pg_node_attr(query_jumble_ignore);
	/* list of column filter expressions */
	List	   *colexprs;
	/* list of column default expressions */
	List	   *coldefexprs pg_node_attr(query_jumble_ignore);
	/* JSON_TABLE: list of column value expressions */
	List	   *colvalexprs pg_node_attr(query_jumble_ignore);
	/* JSON_TABLE: list of PASSING argument expressions */
	List	   *passingvalexprs pg_node_attr(query_jumble_ignore);
	/* nullability flag for each output column */
	Bitmapset  *notnulls pg_node_attr(query_jumble_ignore);
	/* JSON_TABLE plan */
	Node	   *plan pg_node_attr(query_jumble_ignore);
	/* counts from 0; -1 if none specified */
	int			ordinalitycol pg_node_attr(query_jumble_ignore);
	/* token location, or -1 if unknown */
	ParseLoc	location;
} TableFunc;
```
## Detailed Description
TableFunc is a comprehensive node structure designed to represent table functions that transform structured documents (XML or JSON) into relational tabular form. It serves as the foundation for XMLTABLE and JSON_TABLE functionality in PostgreSQL, providing a rich set of capabilities for document processing.

The structure supports namespace handling for XML documents, column type specifications with full metadata (types, type modifiers, collations), filtering expressions at both row and column levels, and default value handling. For JSON_TABLE specifically, it includes additional features like value expressions, passing arguments, and execution plans.

Many fields are marked with pg_node_attr(query_jumble_ignore) to exclude them from query fingerprinting, as they represent metadata rather than core query logic.

## Parameters / Member Variables
- `type`: Standard NodeTag for PostgreSQL's node system type identification
- `functype`: TableFuncType enum indicating XMLTABLE or JSON_TABLE
- `pg_node_attr(query_jumble_ignore)`: List of namespace URI expressions for XML namespace handling
- `pg_node_attr(query_jumble_ignore)`: List of namespace names (String nodes or NULL for DEFAULT)
- `*docexpr`: Expression providing the input document to process
- `*rowexpr`: Expression for filtering/selecting rows from the document
- `pg_node_attr(query_jumble_ignore)`: List of String nodes containing output column names
- `pg_node_attr(query_jumble_ignore)`: List of OIDs representing the data types of output columns
- `pg_node_attr(query_jumble_ignore)`: List of integers specifying type modifiers for columns
- `pg_node_attr(query_jumble_ignore)`: List of OIDs specifying collation for each column
- `*colexprs`: List of expressions for extracting/computing column values
- `pg_node_attr(query_jumble_ignore)`: List of default value expressions for columns
- `pg_node_attr(query_jumble_ignore)`: JSON_TABLE specific: column value extraction expressions
- `pg_node_attr(query_jumble_ignore)`: JSON_TABLE specific: PASSING clause argument expressions
- `pg_node_attr(query_jumble_ignore)`: Bitmapset indicating which columns are NOT NULL
- `pg_node_attr(query_jumble_ignore)`: JSON_TABLE specific: execution plan node
- `pg_node_attr(query_jumble_ignore)`: Index of ordinality column (0-based, -1 if none)
- `location`: Parse location in original query for error reporting
## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (node system)
  - TableFuncType (function type enumeration)
  - [List](../L/List.md) (PostgreSQL list structure)
  - [Node](../N/Node.md) (generic node structure)
  - [Bitmapset](../B/Bitmapset.md) (bitmap utilities)
  - ParseLoc (parse location tracking)

- Called from (representative examples):
  - [transformRangeTableFunc](../t/transformRangeTableFunc.md) (parsing table functions)
  - [transformJsonTable](../t/transformJsonTable.md) (JSON_TABLE parsing)
  - [addRangeTableEntryForTableFunc](../a/addRangeTableEntryForTableFunc.md) (RTE creation)
  - [ExecInitTableFuncScan](../E/ExecInitTableFuncScan.md) (execution initialization)
  - [create_tablefuncscan_plan](../c/create_tablefuncscan_plan.md) (plan creation)
  - get_tablefunc (rule decompilation)

## Notes and Other Information
- Central to PostgreSQL's structured document processing capabilities
- Supports both XMLTABLE and JSON_TABLE SQL standard functions
- Many metadata fields excluded from query jumbling for performance
- Handles complex column specifications with full type system integration
- JSON_TABLE includes additional execution planning and argument passing
- Ordinality columns provide row numbering functionality
- Extensive namespace support for XML document processing
- Critical component of the Range Table Entry system for table functions