CreatedbStmt

Overview: CreatedbStmt represents a CREATE DATABASE statement in PostgreSQL parse tree, containing the database name and creation options.

Definition: typedef struct CreatedbStmt with NodeTag type, char *dbname for database name, and List *options for DefElem nodes.

Description: CreatedbStmt is a parse node structure that represents the CREATE DATABASE SQL statement. It encapsulates the database name to be created along with a list of database creation options such as template, encoding, locale, and other database properties specified in the WITH clause.

Parameters:
- type: Standard NodeTag for parse tree node identification
- dbname: String name of the database to be created
- options: List of DefElem nodes containing database creation options

Dependencies:
Referenced symbols: None
Called from: createdb, standard_ProcessUtility, DBCOMMANDS_H

Notes: CreatedbStmt is processed during utility command execution by createdb() function in src/backend/commands/dbcommands.c. The options list can contain various database creation parameters like TEMPLATE, ENCODING, LC_COLLATE, LC_CTYPE, TABLESPACE, and others. Each option is represented as a DefElem node with name-value pairs.