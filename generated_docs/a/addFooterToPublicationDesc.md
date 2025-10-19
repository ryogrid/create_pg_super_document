# addFooterToPublicationDesc

## Location
[src/bin/psql/describe.c:6293-6338](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L6293-L6338)

## Overview
A static helper function that adds footer information to publication descriptions by executing a query and formatting the results for display.

## Definition

```c
static bool
addFooterToPublicationDesc(PQExpBuffer buf, const char *footermsg,
						   bool as_schema, printTableContent *const cont)
```
## Detailed Description
The  function is a utility function used to add detailed footer information to publication descriptions in psql. It executes a SQL query contained in the provided buffer and formats the results as footer lines in the publication description output. The function handles two different formatting modes: schema-only mode and full table mode with optional column lists and WHERE clauses.

The function performs the following operations:
1. Executes the SQL query from the buffer
2. Counts the number of result rows
3. Adds a header message if results exist
4. Iterates through results and formats each row based on the  flag
5. For schema mode: displays schema names only
6. For table mode: displays table names with optional column lists and WHERE clauses

## Parameters / Member Variables
- `buf`: PQExpBuffer containing the SQL query to execute and used for formatting output strings
- `*footermsg`: Header message to display before the footer content
- `as_schema`: Boolean flag indicating whether to format results as schemas (true) or tables (false)
- `cont`: Pointer to printTableContent structure for adding footer lines to the output
## Dependencies
- Functions called/Symbols referenced:
  - [PSQLexec](../P/PSQLexec.md)
  - [printTableAddFooter](../p/printTableAddFooter.md)
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md)
  - [PQgetisnull](../P/PQgetisnull.md)
  - [PQgetvalue](../P/PQgetvalue.md)
  - [PQntuples](../P/PQntuples.md)
  - [PQclear](../P/PQclear.md)
- Called from (representative examples):
  - [describePublications](../d/describePublications.md) (called twice for different footer sections)

## Notes and Other Information
- This is a static function, only accessible within the describe.c file
- Used specifically for formatting publication-related footer information
- Handles optional column specifications and WHERE clauses for table publications
- The function expects specific column ordering in the query results:
  - Column 0: Schema name (schema mode) or Schema name (table mode)
  - Column 1: Table name (table mode only)
  - Column 2: WHERE clause (optional, table mode only)
  - Column 3: Column list (optional, table mode only)
- Returns boolean indicating success/failure of the operation

## Simplified Source

```c
static bool addFooterToPublicationDesc(PQExpBuffer buf, const char *footermsg,
                                       bool as_schema, printTableContent *const cont) {
    PGresult *res;
    int count = 0;
    int i = 0;

    // Execute the query and get result count
    res = PSQLexec(buf->data);
    if (!res)
        return false;
    else
        count = PQntuples(res);

    // Add footer header if results exist
    if (count > 0)
        printTableAddFooter(cont, footermsg);

    // Process each result row
    for (i = 0; i < count; i++) {
        if (as_schema) {
            // Schema-only format
            printfPQExpBuffer(buf, "    \"%s\"", PQgetvalue(res, i, 0));
        } else {
            // Full table format with schema.table
            printfPQExpBuffer(buf, "    \"%s.%s\"", PQgetvalue(res, i, 0),
                              PQgetvalue(res, i, 1));

            // Add column list if present
            if (!PQgetisnull(res, i, 3))
                appendPQExpBuffer(buf, " (%s)", PQgetvalue(res, i, 3));

            // Add WHERE clause if present
            if (!PQgetisnull(res, i, 2))
                appendPQExpBuffer(buf, " WHERE %s", PQgetvalue(res, i, 2));
        }

        printTableAddFooter(cont, buf->data);
    }

    PQclear(res);
    return true;
}
```