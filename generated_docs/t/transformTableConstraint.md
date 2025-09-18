# transformTableConstraint

## Location
[src/backend/parser/parse_utilcmd.c:903-979](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_utilcmd.c#L903-L979)

## Overview
Transforms table-level constraint nodes within CREATE TABLE or ALTER TABLE statements, categorizing constraints and validating their applicability to different table types.

## Definition


## Detailed Description
transformTableConstraint processes table-level constraints during DDL operations. Unlike column-level constraints handled by transformColumnDefinition, this function deals with constraints that apply to the table as a whole or involve multiple columns. The function performs two main tasks:

1. **Constraint categorization**: Sorts different constraint types into appropriate lists within the CreateStmtContext for later processing in the correct order
2. **Foreign table validation**: Enforces restrictions on which constraint types are supported for foreign tables

The function categorizes constraints into:
- Index constraints (PRIMARY KEY, UNIQUE, EXCLUSION) → ixconstraints list
- Check constraints → ckconstraints list  
- Foreign key constraints → fkconstraints list

For foreign tables, the function enforces PostgreSQL's design decision that certain constraints don't make sense since the data resides on external systems and cannot be enforced by the local PostgreSQL instance.

## Parameters / Member Variables
- : CreateStmtContext containing parsing state and constraint accumulation lists
- : Constraint node representing the table-level constraint to be processed

## Dependencies
- Functions called/Symbols referenced:
  - lappend (for adding constraints to respective lists)
  - ereport (for error reporting)
  - elog (for internal errors)
- Called from (representative examples):
  - [transformCreateStmt](transformCreateStmt.md)
  - [transformAlterTableStmt](transformAlterTableStmt.md)

## Notes and Other Information
The function enforces that column-level constraint types (NULL, NOT NULL, DEFAULT, and constraint attributes like DEFERRABLE) cannot appear as table-level constraints, throwing internal errors if encountered. This separation ensures proper constraint processing order and prevents logical inconsistencies. Foreign tables have significant constraint limitations - they cannot have PRIMARY KEY, UNIQUE, EXCLUSION, or FOREIGN KEY constraints since these require local enforcement capabilities that don't exist for external data sources. The constraint categorization performed here enables later processing phases to handle each constraint type with the appropriate logic and in the correct sequence.