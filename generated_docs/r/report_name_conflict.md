# report_name_conflict

## Location
[src/backend/commands/alter.c:76-110](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/alter.c#L76-L110)

## Overview
A static helper function that raises an error indicating that an object with the given name already exists in the database namespace.

## Definition

```c
static void
report_name_conflict(Oid classId, const char *name)
```
## Detailed Description
This function generates appropriate error messages for duplicate object names based on the object class ID. It uses a switch statement to determine the correct error message format for different types of database objects including event triggers, foreign-data wrappers, foreign servers, languages, publications, and subscriptions. The function then raises an ERROR with the ERRCODE_DUPLICATE_OBJECT error code and the formatted message.

## Parameters / Member Variables
- `classId`: Object identifier (Oid) representing the class/type of the database object that has a naming conflict
- `*name`: String containing the name of the conflicting object
## Dependencies
- Functions called/Symbols referenced:
  - gettext_noop (for internationalization)
  - elog (for error logging)
  - ereport (for error reporting)
  - [errcode](../e/errcode.md) (for error code specification)
  - [errmsg](../e/errmsg.md) (for error message formatting)
  - ERRCODE_DUPLICATE_OBJECT (error code constant)

- Called from (representative examples):
  - [AlterObjectRename_internal](../A/AlterObjectRename_internal.md) (src/backend/commands/alter.c:298)
  - [AlterObjectRename_internal](../A/AlterObjectRename_internal.md) (src/backend/commands/alter.c:322)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the same source file (src/backend/commands/alter.c)
- The function supports only specific object types; unsupported object classes will trigger an ERROR with elog
- Uses PostgreSQL's internationalization framework with gettext_noop for translatable error messages
- Part of the object renaming and alteration subsystem in PostgreSQL

## Simplified Source

```c
static void report_name_conflict(Oid classId, const char *name)
{
    char *msgfmt;

    // Determine appropriate error message based on object type
    switch (classId)
    {
        case EventTriggerRelationId:
            msgfmt = gettext_noop("event trigger \"%s\" already exists");
            break;
        case ForeignDataWrapperRelationId:
            msgfmt = gettext_noop("foreign-data wrapper \"%s\" already exists");
            break;
        case ForeignServerRelationId:
            msgfmt = gettext_noop("server \"%s\" already exists");
            break;
        case LanguageRelationId:
            msgfmt = gettext_noop("language \"%s\" already exists");
            break;
        case PublicationRelationId:
            msgfmt = gettext_noop("publication \"%s\" already exists");
            break;
        case SubscriptionRelationId:
            msgfmt = gettext_noop("subscription \"%s\" already exists");
            break;
        default:
            elog(ERROR, "unsupported object class: %u", classId);
            break;
    }

    // Report duplicate object error
    ereport(ERROR,
            (errcode(ERRCODE_DUPLICATE_OBJECT),
             errmsg(msgfmt, name)));
}
```