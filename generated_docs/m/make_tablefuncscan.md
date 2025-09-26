# make_tablefuncscan

## Location
[src/backend/optimizer/plan/createplan.c:5725-5743](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L5725-L5743)

## Overview
Creates and initializes a TableFuncScan plan node, which represents a scan operation on the result set returned by table functions like XMLTABLE, JSON_TABLE, or other structured data extraction functions in PostgreSQL's query execution plan.

## Definition
```c
static TableFuncScan *
make_tablefuncscan(List *qptlist,
                   List *qpqual,
                   Index scanrelid,
                   TableFunc *tablefunc)
```

## Detailed Description
The `make_tablefuncscan` function is a factory function that constructs a TableFuncScan plan node. This node type is used when the query planner needs to scan the results of table functions that extract structured data from documents or other complex data sources. Table functions like XMLTABLE and JSON_TABLE are examples of functionality that would use this scan type. The function allocates memory for a new TableFuncScan node, initializes its base Plan structure with the provided target list and qualification conditions, and stores the TableFunc structure that contains the details about how to extract and format the data.

## Parameters / Member Variables
- `qptlist`: The target list (projection list) specifying which columns/expressions to return from the table function scan
- `qpqual`: The qualification conditions (WHERE clause predicates) to be applied during the scan
- `scanrelid`: The relation ID assigned to this scan operation for identification purposes
- `tablefunc`: A TableFunc structure containing the specific details about the table function to execute, including data source, column definitions, and extraction rules

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to allocate TableFuncScan node)
  - [TableFuncScan](../T/TableFuncScan.md) (node type)
  - [TableFunc](../T/TableFunc.md) (structure type)
- Called from (representative examples):
  - [create_tablefuncscan_plan](../c/create_tablefuncscan_plan.md)

## Notes and Other Information
- This is a static function within createplan.c, indicating it's an internal helper for plan creation
- The function follows PostgreSQL's pattern of setting lefttree and righttree to NULL for leaf scan nodes
- [TableFuncScan](../T/TableFuncScan.md) is primarily used for SQL/XML and SQL/JSON functionality where structured data is extracted from documents
- The TableFunc structure contains all the necessary information about column definitions, namespaces, and data extraction logic
- Part of PostgreSQL's query planner infrastructure that handles advanced table function operations for document processing