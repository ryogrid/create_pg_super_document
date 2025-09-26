AlterDatabaseRefreshCollStmt

Overview: AlterDatabaseRefreshCollStmt represents an ALTER DATABASE REFRESH COLLATION VERSION statement for updating collation version information in PostgreSQL.

Definition: typedef struct AlterDatabaseRefreshCollStmt with NodeTag type and char *dbname for the target database name.

Description: AlterDatabaseRefreshCollStmt is a parse node structure that represents the ALTER DATABASE REFRESH COLLATION VERSION SQL statement. This specialized statement is used to refresh the stored collation version information for a database when the underlying operating system collation libraries have been updated. It helps maintain consistency between PostgreSQL collation version tracking and the actual system collation versions.

Parameters:
- type: Standard NodeTag for parse tree node identification  
- dbname: String name of the database whose collation version should be refreshed

Dependencies:
Referenced symbols: None
Called from: AlterDatabaseRefreshColl, standard_ProcessUtility, DBCOMMANDS_H

Notes: AlterDatabaseRefreshCollStmt is processed by AlterDatabaseRefreshColl() function in src/backend/commands/dbcommands.c during utility command execution. This statement is particularly important when system collation libraries are upgraded, as PostgreSQL tracks collation versions to detect potential inconsistencies. The command updates the stored collation version information to match the current system versions, preventing warnings about version mismatches.