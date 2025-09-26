AlterDatabaseStmt

Overview: AlterDatabaseStmt represents an ALTER DATABASE statement in PostgreSQL parse tree, containing the database name and alteration options.

Definition: typedef struct AlterDatabaseStmt with NodeTag type, char *dbname for database name, and List *options for DefElem nodes.

Description: AlterDatabaseStmt is a parse node structure that represents the ALTER DATABASE SQL statement. It allows modification of database properties such as renaming the database, changing owner, setting configuration parameters, or modifying other database attributes. The structure contains the target database name and a list of options specifying the changes to be made.

Parameters:
- type: Standard NodeTag for parse tree node identification
- dbname: String name of the database to be altered
- options: List of DefElem nodes containing alteration options and their values

Dependencies:
Referenced symbols: None
Called from: AlterDatabase, standard_ProcessUtility, DBCOMMANDS_H

Notes: AlterDatabaseStmt is processed during utility command execution by AlterDatabase() function in src/backend/commands/dbcommands.c. The options can include operations like RENAME TO, OWNER TO, SET configuration parameters, RESET configuration parameters, CONNECTION LIMIT changes, and other database property modifications. Each option is represented as a DefElem node with appropriate name-value pairs.