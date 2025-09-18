# quote_identifier

## Location
[src/bin/pg_upgrade/util.c:299-322](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/util.c#L299-L322)

## Overview
quote_identifier is a PostgreSQL backend utility function that conditionally adds double quotes around SQL identifiers when necessary to preserve their case sensitivity or to handle reserved keywords and special characters.

## Definition


## Detailed Description
quote_identifier analyzes a SQL identifier string and determines whether it needs to be quoted with double quotes according to SQL standards. The function implements PostgreSQL's identifier quoting rules: identifiers are quoted if they contain uppercase letters, special characters (other than underscores), start with a digit, or match SQL reserved keywords. The function performs case-insensitive keyword lookups using PostgreSQL's keyword scanning system and only quotes keywords that are not unreserved. When quoting is needed, the function handles proper escaping by doubling any internal quote characters. If the global quote_all_identifiers setting is enabled, all identifiers are quoted regardless of other conditions. The function returns either the original identifier (if safe) or a newly allocated quoted version.

## Parameters / Member Variables
- : A null-terminated string containing the SQL identifier to potentially quote

## Dependencies
- Functions called/Symbols referenced:
  - ScanKeywordLookup (performs case-insensitive keyword matching)
  - ScanKeywords (global keyword list structure)
  - ScanKeywordCategories (array of keyword categories)
  - UNRESERVED_KEYWORD (enum value for unreserved keywords)
  - [palloc](../p/palloc.md) (PostgreSQL's memory allocation function)
- Global variables accessed:
  - quote_all_identifiers (forces quoting of all identifiers when true)
- Called from (representative examples):
  - [quote_ident](quote_ident.md) (SQL function wrapper)
  - [pg_get_triggerdef_worker](../p/pg_get_triggerdef_worker.md) (trigger definition generation)
  - [pg_get_indexdef_worker](../p/pg_get_indexdef_worker.md) (index definition generation)
  - get_rule_expr (rule expression formatting)
  - get_from_clause_item (FROM clause formatting)
  - Various ruleutils.c functions for SQL reconstruction

## Notes and Other Information
- Part of PostgreSQL's SQL reconstruction and formatting system in ruleutils.c
- Uses manual character classification instead of ctype.h to avoid locale-specific behavior
- Handles proper escaping of internal double quotes by doubling them
- Memory for quoted identifiers is allocated using palloc and managed by PostgreSQL's memory contexts
- The function is extensively used throughout the backend for generating SQL text from internal structures
- Critical for maintaining SQL standard compliance in identifier handling
- Performance optimized with early checks for simple safe cases
- Located in src/backend/utils/adt/ruleutils.c:12699-12782