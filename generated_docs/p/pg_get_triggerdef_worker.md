# pg_get_triggerdef_worker

## Location
[src/backend/utils/adt/ruleutils.c:880-1157](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L880-L1157)

## Overview
A static worker function that constructs the complete CREATE TRIGGER statement for a given trigger OID by querying the system catalog and building the DDL string.

## Definition

```c
static char *
pg_get_triggerdef_worker(Oid trigid, bool pretty)
```
## Detailed Description
This function performs the core work of reconstructing a trigger definition from PostgreSQL's system catalogs. It queries the pg_trigger system table to retrieve trigger metadata, then constructs a complete CREATE TRIGGER statement including timing (BEFORE/AFTER/INSTEAD OF), events (INSERT/UPDATE/DELETE/TRUNCATE), target table, constraint information, transition table references, row/statement level specification, WHEN clause if present, and the trigger function with arguments. The function handles both pretty-printed output (with selective schema qualification) and non-pretty output (with full schema qualification for safety).

## Parameters / Member Variables
- `trigid`: OID of the trigger to retrieve the definition for
- `pretty`: Boolean flag controlling output formatting - when true, uses readable formatting and selective schema qualification; when false, always uses full schema qualification
## Dependencies
- Functions called/Symbols referenced:
  - : Opens the pg_trigger system catalog
  - : Initializes scan key for trigger lookup
  - : Begins system catalog scan
  - : Gets next tuple from scan
  - : Validates retrieved tuple
  - : Casts tuple to trigger struct
  - : Initializes string buffer
  - : Appends formatted text to buffer
  - : Quotes SQL identifiers for safety
  -  macros: Tests trigger timing and events
  - : Retrieves column names for UPDATE OF triggers
  - : Gets relation name with optional schema qualification
  - : Extracts attributes from heap tuple
  - : Parses stored expression text
  - : Deparses expressions for WHEN clause
  - : Gets trigger function name
  - : Quotes string literals
- Called from (representative examples):
  - : Standard trigger definition function
  - : Extended trigger definition function

## Notes and Other Information
- Handles all trigger types: row-level, statement-level, constraint triggers
- Supports WHEN clause reconstruction with proper variable context (OLD/NEW)
- Manages transition table references (REFERENCING OLD TABLE AS/NEW TABLE AS)
- Processes trigger arguments embedded in tgargs bytea field
- Uses deparse context to properly format complex WHEN expressions
- Returns NULL if trigger OID is not found in system catalog
- Part of PostgreSQL's rule utilities for DDL reconstruction
- Located in src/backend/utils/adt/ruleutils.c:880-1157
- Critical component for pg_dump, system introspection, and trigger administration