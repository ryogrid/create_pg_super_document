# storeObjectDescription

## Location
[src/backend/catalog/pg_shdepend.c:1276-1341](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_shdepend.c#L1276-L1341)

## Overview
Formats and appends descriptive text for dependent objects to a string buffer, creating human-readable dependency descriptions for error messages when shared objects cannot be dropped.

## Definition

```c
static void
storeObjectDescription(StringInfo descs,
					   SharedDependencyObjectType type,
					   ObjectAddress *object,
					   SharedDependencyType deptype,
					   int count)
```
## Detailed Description
This function constructs human-readable descriptions of database objects that depend on shared objects (like roles, tablespaces, databases). It formats dependency relationships in user-friendly language that can be displayed in error messages when attempting to drop referenced shared objects.

The function handles three types of objects differently: LOCAL_OBJECT and SHARED_OBJECT use dependency type information to describe the nature of the relationship (owner, privileges, etc.), while REMOTE_OBJECT uses a count to describe multiple objects in a remote database. The formatted descriptions are accumulated in a StringInfo buffer with newline separators.

## Parameters / Member Variables
- : StringInfo buffer to append the formatted description to
- : Type of dependent object (LOCAL_OBJECT, SHARED_OBJECT, or REMOTE_OBJECT)
- : ObjectAddress structure identifying the dependent object
- : Type of dependency relationship (used for LOCAL/SHARED objects only)
- : Number of objects for REMOTE_OBJECT type (ignored for other types)

## Dependencies
- Functions called/Symbols referenced:
  - [getObjectDescription](../g/getObjectDescription.md)
  - appendStringInfoChar
  - appendStringInfo
  - ngettext
  - [pfree](../p/pfree.md)
  - elog
  - _ (gettext macro)
- Called from (representative examples):
  - Various dependency checking functions in pg_shdepend.c (via MAX_REPORTED_DEPS context)

## Notes and Other Information
- This is a static internal function, not directly accessible outside pg_shdepend.c
- Handles internationalization using gettext macros (_ and ngettext)
- Gracefully handles objects being dropped concurrently (returns early if getObjectDescription returns NULL)
- Formats different dependency types with specific wording:
  - SHARED_DEPENDENCY_OWNER: "owner of %s"
  - SHARED_DEPENDENCY_ACL: "privileges for %s"
  - SHARED_DEPENDENCY_INITACL: "initial privileges for %s"
  - SHARED_DEPENDENCY_POLICY: "target of %s"
  - SHARED_DEPENDENCY_TABLESPACE: "tablespace for %s"
- Uses plural forms correctly for REMOTE_OBJECT via ngettext
- Properly manages memory by freeing the object description string
- Entries are separated with newlines for multi-line error messages