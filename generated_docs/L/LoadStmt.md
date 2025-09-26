LoadStmt

Overview: LoadStmt represents a LOAD statement in PostgreSQL parse tree for dynamically loading shared libraries into the server process.

Definition: typedef struct LoadStmt with NodeTag type and char *filename for the library file to load.

Description: LoadStmt is a simple parse node structure that represents the LOAD SQL statement used to dynamically load shared library files into the PostgreSQL server process. This is primarily used for loading extension modules and procedural languages at runtime.

Parameters:
- type: Standard NodeTag for parse tree node identification
- filename: String path to the shared library file to be loaded

Dependencies:
Referenced symbols: None
Called from: standard_ProcessUtility

Notes: LoadStmt is a simple structure used for the LOAD command which allows dynamic loading of shared libraries. Processed during utility command execution phase by standard_ProcessUtility() in src/backend/tcop/utility.c. Used primarily for loading extensions and procedural language handlers.