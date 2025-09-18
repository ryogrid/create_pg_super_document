# appendQualifiedRelation

## Location
[src/bin/scripts/common.c:69-131](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/scripts/common.c#L69-L131)

## Overview
Resolves a table specification string to its fully qualified name and appends it to a buffer, ensuring the table name is properly qualified with schema information.

## Definition


## Detailed Description
This function takes a table specification in the format TABLE[(COLUMNS)] and resolves the TABLE portion to its fully qualified schema.tablename format using the current search_path. It performs a database query to look up the table's actual schema and name, then appends the qualified result along with any column specification to the provided buffer.

The function first uses splitTableColumnsSpec to separate the table name from any column specification. It then executes a query against the PostgreSQL system catalogs to resolve the table name to its fully qualified form (schema.table). The query uses regclass casting to handle the table name resolution according to the current search_path, then retrieves both the relation name and namespace name from the catalog tables.

After obtaining the qualified name, it formats it properly using fmtQualifiedIdEnc and appends it to the buffer along with any column specification that was part of the original spec. The function ensures security by resetting the search_path and using the ALWAYS_SECURE_SEARCH_PATH_SQL at the end.

## Parameters / Member Variables
- : PQExpBuffer to append the qualified relation name to
- : Input specification string in format TABLE[(COLUMNS)]
- : Active PostgreSQL database connection
- : Whether to echo executed SQL commands

## Dependencies
- Functions called/Symbols referenced:
  - [splitTableColumnsSpec](../s/splitTableColumnsSpec.md)
  - [PQclientEncoding](../P/PQclientEncoding.md)
  - initPQExpBuffer
  - [appendPQExpBufferStr](appendPQExpBufferStr.md)
  - [appendStringLiteralConn](appendStringLiteralConn.md)
  - [executeCommand](../e/executeCommand.md)
  - [executeQuery](../e/executeQuery.md)
  - [PQntuples](../P/PQntuples.md)
  - [PQgetvalue](../P/PQgetvalue.md)
  - [fmtQualifiedIdEnc](../f/fmtQualifiedIdEnc.md)
  - [PQclear](../P/PQclear.md)
  - termPQExpBuffer
  - [pg_free](../p/pg_free.md)
  - ALWAYS_SECURE_SEARCH_PATH_SQL
- Called from (representative examples):
  - [cluster_one_database](../c/cluster_one_database.md)
  - [gen_reindex_command](../g/gen_reindex_command.md)
  - [get_parallel_object_list](../g/get_parallel_object_list.md)

## Notes and Other Information
- The function exits the program on failure if the table cannot be resolved or if an unexpected number of rows is returned
- Uses regclass casting to resolve table names according to PostgreSQL's search_path mechanism
- Handles the security concern of unqualified names by using catalog queries that are devoid of unqualified references
- Resets search_path before performing the resolution to ensure consistent behavior
- Properly handles multi-byte character encodings by using the connection's client encoding
- The resulting qualified name is suitable for use in SQL commands that will be executed under a secure search_path