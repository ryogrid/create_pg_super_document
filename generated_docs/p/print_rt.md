# print_rt

## Location
[src/backend/nodes/print.c:254-320](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/print.c#L254-L320)

## Overview
A debugging utility function that prints the contents of a PostgreSQL range table in a tabular format to stdout.

## Definition
```c
void print_rt(const List *rtable)
```

## Detailed Description
The `print_rt` function is a specialized debugging utility that displays the contents of a PostgreSQL range table (rtable) in a structured, human-readable tabular format. It iterates through each RangeTblEntry in the list and displays key information including the resource number, reference name, relation ID, and various flags. The function handles different types of range table entries (relations, subqueries, joins, functions, etc.) and formats them appropriately with descriptive labels.

## Parameters / Member Variables
- `rtable`: A const pointer to a List containing RangeTblEntry objects representing the range table to be displayed

## Dependencies
- Functions called/Symbols referenced:
  - foreach: PostgreSQL macro for iterating through lists
  - lfirst: PostgreSQL macro for accessing list cell contents
  - printf: Standard C library function for formatted output
  - RTE_RELATION: Enum value for regular relation entries
  - RTE_SUBQUERY: Enum value for subquery entries
  - RTE_JOIN: Enum value for join entries
  - RTE_FUNCTION: Enum value for function entries
  - RTE_TABLEFUNC: Enum value for table function entries
  - RTE_VALUES: Enum value for values list entries
  - RTE_CTE: Enum value for common table expression entries
  - RTE_NAMEDTUPLESTORE: Enum value for named tuplestore entries
  - RTE_RESULT: Enum value for result entries

- Called from (representative examples):
  - nodeDisplay: Header declaration and debugging macros

## Notes and Other Information
- Displays a header with column names: "resno", "refname", "relid", "inFromCl"
- Handles all known RangeTblEntry types with appropriate labels
- Shows inheritance (inh) and inFromCl flags for each entry
- Uses descriptive labels like [subquery], [join], [rangefunction] for non-relation entries
- Primarily used for debugging query planning and range table analysis
- Output format is tabular for easy reading and analysis
- Each entry is numbered sequentially starting from 1
- Located in src/backend/nodes/print.c:254-320