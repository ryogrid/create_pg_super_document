# set_locale_and_encoding

## Location
[src/bin/pg_upgrade/pg_upgrade.c:405-483](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/pg_upgrade.c#L405-L483)

## Overview
Copies locale and encoding information from the old cluster's template0 database to the new cluster's template0, ensuring consistent locale settings during pg_upgrade.

## Definition

```c
static void
set_locale_and_encoding(void)
```
## Detailed Description
The set_locale_and_encoding function transfers critical locale and encoding configuration from the old cluster to the new cluster by updating the template0 database in the new cluster. This ensures that all databases created from template0 will inherit the correct locale settings. The function:

- Extracts locale information from the old cluster's template0 database
- Connects to the new cluster's template1 database to perform updates
- Escapes locale strings safely using PQescapeLiteral 
- Updates encoding, datlocprovider, datcollate, datctype, and datlocale fields
- Handles version-specific differences in locale field names (datlocale vs daticulocale)
- Supports PostgreSQL versions 15.0+ with locale provider functionality and 17.0+ with enhanced locale handling

The function is version-aware and adapts the SQL update statement based on the target PostgreSQL version.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - [prep_status](../p/prep_status.md) (status reporting)
  - [connectToServer](../c/connectToServer.md) (database connection)
  - [PQescapeLiteral](../P/PQescapeLiteral.md) (SQL string escaping)
  - [executeQueryOrDie](../e/executeQueryOrDie.md) (SQL execution)
  - GET_MAJOR_VERSION (version checking)
  - [PQfreemem](../P/PQfreemem.md) (memory cleanup)
  - [PQfinish](../P/PQfinish.md) (connection cleanup)
  - [check_ok](../c/check_ok.md) (status verification)
  - DbLocaleInfo (locale information structure)
- Called from:
  - [main](../m/main.md) (from pg_upgrade.c:151)

## Notes and Other Information
- Critical for maintaining locale consistency across PostgreSQL versions
- Handles version differences between PostgreSQL 15+ (daticulocale) and 17+ (datlocale)
- Does not copy datcollversion as it's never set for template0
- Uses proper SQL escaping to prevent injection issues with locale strings
- Essential for ensuring that new databases created after upgrade have correct locale settings
- Part of the schema transfer phase of pg_upgrade process