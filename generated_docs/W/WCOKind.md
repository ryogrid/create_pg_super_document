# WCOKind

## Location
src/include/nodes/parsenodes.h: 1366 - 1367

## Overview
WCOKind is an enumeration that defines the different types of WITH CHECK OPTION constraints that can be applied to tuples during INSERT/UPDATE operations on auto-updatable views or relations with Row Level Security (RLS) policies.

## Definition


## Detailed Description
WCOKind classifies different types of WITH CHECK OPTION constraints in PostgreSQL. These constraints ensure that newly inserted or updated tuples satisfy certain conditions. The enumeration distinguishes between view-level checks and various Row Level Security (RLS) policy checks that apply in different contexts such as INSERT, UPDATE, MERGE, and conflict resolution scenarios.

## Parameters / Member Variables
- : WITH CHECK OPTION constraint on an auto-updatable view
- : Row Level Security INSERT WITH CHECK policy constraint
- : Row Level Security UPDATE WITH CHECK policy constraint  
- : Row Level Security constraint for ON CONFLICT DO UPDATE operations
- : Row Level Security constraint for MERGE UPDATE operations
- : Row Level Security constraint for MERGE DELETE operations

## Dependencies
- Functions called/Symbols referenced:
  - (None - this is an enumeration type)
- Called from (representative examples):
  - ExecWithCheckOptions (src/backend/executor/execMain.c:2053)
  - ExecInsert (src/backend/executor/nodeModifyTable.c:978)
  - add_with_check_options (src/backend/rewrite/rowsecurity.c:798)
  - ExecGetJunkAttribute (src/include/executor/executor.h:230)
  - WithCheckOption (src/include/nodes/parsenodes.h:1371)

## Notes and Other Information
- WCOKind is essential for PostgreSQL's security and data integrity mechanisms
- View-level WITH CHECK OPTION ensures that modifications through views maintain consistency
- RLS (Row Level Security) variants provide fine-grained access control at the tuple level
- Different WCO types are enforced at different stages of query execution
- MERGE operations introduced additional WCO types for comprehensive policy enforcement