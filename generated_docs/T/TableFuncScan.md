# TableFuncScan

## Location
[src/include/nodes/plannodes.h:630-634](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/plannodes.h#L630-L634)

## Overview
TableFuncScan represents a plan node for scanning table functions in PostgreSQL's query execution tree, providing specialized support for SQL/JSON table functions and XML table functions.

## Definition
```c
typedef struct TableFuncScan
{
    Scan        scan;
    TableFunc  *tablefunc;      /* table function node */
} TableFuncScan;
```

## Detailed Description
TableFuncScan is a specialized plan node that handles the execution of table functions, particularly those defined by SQL standards like JSON_TABLE and XMLTABLE functions. It extends the base Scan node to provide functionality for processing structured data (JSON or XML) and transforming it into relational format according to specified column definitions and path expressions.

This node type is essential for modern SQL features that allow querying semi-structured data directly within SQL. The TableFunc structure contains detailed information about the table function specification, including input data sources, column definitions, path expressions, and transformation rules. During execution, the node processes the input data and generates tuples according to the table function's specification.

## Parameters / Member Variables
- `scan`: Base Scan structure containing common scanning information like target lists, qualifications, and plan node metadata
- `tablefunc`: Pointer to TableFunc structure containing the complete specification of the table function including input sources, column definitions, and processing parameters

## Dependencies
- Functions called/Symbols referenced:
  - Scan (base structure)
  - TableFunc (table function specification)
  
- Called from (representative examples):
  - ExecInitTableFuncScan (executor initialization)
  - tfuncInitialize (table function initialization)
  - tfuncLoadRows (table function row loading)
  - create_tablefuncscan_plan (plan creation)
  - make_tablefuncscan (plan node construction)
  - JsonTableInitOpaque (JSON table function support)

## Notes and Other Information
- Critical for implementing SQL/JSON and XML table function standards
- Supports complex data transformation from semi-structured to relational format
- Integrates with PostgreSQL's JSON and XML processing capabilities
- Part of the modern SQL standard compliance for handling non-relational data
- Used extensively in scenarios involving JSON_TABLE, XMLTABLE, and similar functions
- Provides efficient processing of nested and hierarchical data structures
- Essential for data integration scenarios where JSON or XML data needs to be queried relationally