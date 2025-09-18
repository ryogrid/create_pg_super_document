# AlterTablePass

## Location
src/backend/commands/tablecmds.c: 162 - 163

## Overview
AlterTablePass is an enumeration that defines the execution phases for ALTER TABLE commands in PostgreSQL, ensuring operations are performed in the correct order to maintain data integrity and consistency.

## Definition


## Detailed Description
The AlterTablePass enumeration is a critical component of PostgreSQL's ALTER TABLE command processing system. It defines a sequential execution framework that ensures ALTER TABLE operations are performed in the correct order to avoid dependency conflicts and maintain referential integrity.

The enumeration establishes a multi-pass execution strategy where different types of table alterations are grouped into logical phases. This approach prevents issues that could arise from attempting to perform conflicting operations simultaneously, such as trying to add a constraint that references a column before that column has been added.

Each pass represents a distinct phase in the ALTER TABLE execution pipeline, with operations ordered from most destructive (DROP operations) to least disruptive (miscellaneous operations). This ordering ensures that dependencies are properly managed throughout the table alteration process.

## Parameters / Member Variables
- : Invalid state marker that will trigger an error if encountered during execution
- : Handles all DROP operations (columns, constraints, indexes) - executed first to remove dependencies
- : Processes ALTER COLUMN TYPE operations that may require table rewrites
- : Adds new columns to the table structure
- : Handles ALTER SET EXPRESSION operations for generated columns
- : Re-creates existing indexes that may have been affected by previous operations
- : Re-creates existing constraints that may have been affected by previous operations
- : Initial examination and preparation for adding new constraints
- : Sets column attributes such as NOT NULL, DEFAULT values, and other column properties
- : Adds constraints that are backed by indexes (PRIMARY KEY, UNIQUE)
- : Creates new indexes on the table
- : Adds other types of constraints and default values
- : Handles miscellaneous operations that don't fit into other categories

## Dependencies
- Functions called/Symbols referenced:
  - Used as a type parameter in various ALTER TABLE processing functions
  - No direct function calls (enumeration type)

- Called from (representative examples):
  -  (src/backend/commands/tablecmds.c:420, 425, 448, 594)
  -  (src/backend/commands/tablecmds.c:4784)
  -  (src/backend/commands/tablecmds.c:5170)
  -  (src/backend/commands/tablecmds.c:5233)
  -  (src/backend/commands/tablecmds.c:5569, 5607)
  -  (src/backend/commands/tablecmds.c:7014)
  -  (src/backend/commands/tablecmds.c:14247)

## Notes and Other Information
- The enumeration is defined in src/backend/commands/tablecmds.c:148-163
- The pass system is designed to handle complex ALTER TABLE operations that involve multiple interdependent changes
- The ordering of passes is critical for maintaining database consistency and avoiding constraint violations
- A potential RENAME COLUMN pass is mentioned in comments but is not currently implemented
- The multi-pass approach allows PostgreSQL to handle complex table alterations that would otherwise require multiple separate ALTER TABLE statements
- Each pass may involve different levels of table locking and may trigger table rewrites depending on the operations involved