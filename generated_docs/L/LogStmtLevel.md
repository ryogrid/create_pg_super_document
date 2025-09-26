# LogStmtLevel

## Location
src/include/tcop/tcopprot.h: 41 - 45

## Overview
LogStmtLevel is an enumeration type that defines the different levels of SQL statement logging in PostgreSQL, controlling which types of statements should be logged based on their classification.

## Definition


## Detailed Description
The LogStmtLevel enum provides a hierarchical classification system for SQL statement logging in PostgreSQL. It is used primarily by the GetCommandLogLevel() function to determine the minimum log level required for a given SQL command. The enum values represent increasingly inclusive logging levels:

- **LOGSTMT_NONE**: No statements are logged
- **LOGSTMT_DDL**: Only Data Definition Language statements (CREATE, ALTER, DROP, etc.) are logged
- **LOGSTMT_MOD**: Both modification statements (INSERT, UPDATE, DELETE, MERGE, TRUNCATE) and DDL statements are logged
- **LOGSTMT_ALL**: All statements including SELECT, administrative commands, and utility statements are logged

This hierarchical design allows PostgreSQL administrators to configure logging granularity based on their auditing and debugging needs. The classification is used in conjunction with the log_statement configuration parameter to control which statements actually get logged to the PostgreSQL log files.

## Parameters / Member Variables
- : Represents the lowest logging level where no SQL statements are logged
- : Logging level for Data Definition Language statements that modify database structure
- : Logging level for data modification statements plus all DDL statements
- : Highest logging level that captures all SQL statements and utility commands

## Dependencies
- Functions called/Symbols referenced:
  - None (this is an enum definition)
- Called from (representative examples):
  - GetCommandLogLevel (src/backend/tcop/utility.c:3249)
  - CreateCommandTag (src/backend/tcop/utility.c:3246)
  - CreateCommandName (src/include/tcop/utility.h:108)

## Notes and Other Information
- The enum is defined in src/include/tcop/tcopprot.h:35-41, making it accessible throughout the PostgreSQL codebase
- The GetCommandLogLevel() function uses this enum extensively to classify different types of SQL statements and parse tree nodes
- Individual enum values (LOGSTMT_NONE, LOGSTMT_DDL, LOGSTMT_MOD, LOGSTMT_ALL) are used directly in switch statements within GetCommandLogLevel()
- The design follows PostgreSQL's principle of providing fine-grained control over logging behavior for different statement types
- This enum works in conjunction with the log_statement GUC (Grand Unified Configuration) parameter to determine actual logging behavior
- The hierarchical nature means that higher levels include all statements from lower levels (e.g., LOGSTMT_MOD includes both modification and DDL statements)