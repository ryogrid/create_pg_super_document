# AlterPublicationAction

## Location
src/include/nodes/parsenodes.h: 4174 - 4175

## Overview
AlterPublicationAction is an enumeration that specifies the type of modification action to perform on objects within a PostgreSQL logical replication publication.

## Definition


## Detailed Description
This enumeration defines the different types of actions that can be performed when altering a PostgreSQL publication using the ALTER PUBLICATION statement. Publications are used in logical replication to define which tables should be replicated to subscribers. The enum provides three fundamental operations: adding objects to an existing publication, removing objects from a publication, and completely replacing the publication's object list.

## Parameters / Member Variables
- : Add new tables or objects to the existing publication without removing existing ones
- : Remove specific tables or objects from the publication
- : Replace the entire list of objects in the publication with a new list

## Dependencies
- Functions called/Symbols referenced:
  - (None - this is an enum definition)
- Called from (representative examples):
  - AlterPublicationStmt (as the 'action' field)
  - Parser grammar rules in gram.y for ALTER PUBLICATION statements
  - [Publication](../P/Publication.md) command functions in src/backend/commands/publicationcmds.c

## Notes and Other Information
- This enum is part of PostgreSQL's logical replication infrastructure
- Used specifically in ALTER PUBLICATION statements (ADD TABLE, DROP TABLE, SET TABLE)
- The SET action completely replaces the publication's object list, while ADD and DROP are incremental operations
- Works in conjunction with AlterPublicationStmt structure to represent parsed ALTER PUBLICATION commands
- Located in src/include/nodes/parsenodes.h as part of the SQL parsing framework
- The action determines how the publicationcmds.c functions process the publication modification