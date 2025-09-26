ViewStmt

Overview: ViewStmt represents a CREATE VIEW statement in PostgreSQL parse tree, encapsulating all information needed to create a new view or replace an existing one.

Definition: typedef struct ViewStmt with NodeTag type, RangeVar view, List aliases, Node query, bool replace, List options, ViewCheckOption withCheckOption.

Description: ViewStmt is a parse node structure for CREATE VIEW SQL statements. Contains view name, column aliases, underlying query, replacement semantics, and check options. Used during utility command processing to create views in database catalog.

Parameters:
- type: Standard NodeTag for parse tree identification  
- view: RangeVar pointer for view name and schema
- aliases: List of target column names for view
- query: Underlying SELECT query as raw parse tree
- replace: Boolean for CREATE OR REPLACE semantics
- options: List of WITH clause options
- withCheckOption: WITH CHECK OPTION constraint type

Dependencies:
Referenced symbols: RangeVar, ViewCheckOption
Called from: DefineView, transformCreateSchemaStmtElements, ProcessUtilitySlow, VIEW_H

Notes: Processed by DefineView() in src/backend/commands/view.c during utility command execution. Supports full CREATE VIEW syntax including column aliases and WITH CHECK OPTION constraints.