# patternToSQLRegex

## Location
src/fe_utils/string_utils.c: 1225 - 1393

## Overview
Transforms a shell-style object name pattern into SQL-style regular expressions, handling qualified names, case conversion, and wildcard character mapping.

## Definition


## Detailed Description
This complex function converts shell-style patterns (with wildcards like * and ?) into PostgreSQL-compatible regular expressions. It can parse qualified object names with up to three components (database.schema.name) and handles various formatting requirements:

Key transformations:
- Converts shell wildcards: '*' → '.*', '?' → '.'  
- Lower-cases unquoted uppercase letters
- Escapes regex special characters when inside quotes or when force_escape is true
- Handles quoted identifiers with PostgreSQL-style double-quote escaping
- Splits qualified names at dots into separate components
- Wraps results in '^(...)$' for whole-string matching

The function intelligently distributes pattern components across the provided buffers based on the number of separators found and available output buffers.

## Parameters / Member Variables
- : Character encoding for the input pattern
- : Output buffer for database name pattern (can be NULL)
- : Output buffer for schema name pattern (can be NULL) 
- : Output buffer for object name pattern (can be NULL)
- : Input shell-style pattern string
- : If true, always escape regex special characters
- : If true, don't convert dbname to regex format
- : Output parameter receiving count of separators found

## Dependencies
- Functions called/Symbols referenced:
  - [PQExpBufferData](../P/PQExpBufferData.md)
  - initPQExpBuffer
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - appendPQExpBufferChar
  - termPQExpBuffer
  - [pg_tolower](pg_tolower.md)
  - [PQmblenBounded](../P/PQmblenBounded.md)
  - strchr
  - isupper

- Called from (representative examples):
  - [append_database_pattern](../a/append_database_pattern.md) (src/bin/pg_amcheck/pg_amcheck.c:1363)
  - [append_schema_pattern](../a/append_schema_pattern.md) (src/bin/pg_amcheck/pg_amcheck.c:1396)
  - [append_relation_pattern_helper](../a/append_relation_pattern_helper.md) (src/bin/pg_amcheck/pg_amcheck.c:1441)
  - WHEREAND (src/fe_utils/string_utils.c:1093)

## Notes and Other Information
- Located in src/fe_utils/string_utils.c:1225-1393
- Handles multi-byte character encodings through PQmblenBounded
- Always quotes '$' characters as they're valid in SQL identifiers
- Special handling for '[]' patterns to avoid regex bracket expressions
- Callers should check dotcnt return value for error handling when too many dots are present
- Used primarily by PostgreSQL utilities for pattern matching in database object queries
- Complex state machine handles quote processing and component separation
- Part of frontend utilities library for advanced pattern processing