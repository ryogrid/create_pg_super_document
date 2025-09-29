# DescribeLockTag

## Location
[src/backend/storage/lmgr/lmgr.c:1239-1335](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lmgr.c#L1239-L1335)

## Overview
Generates a human-readable description of a lockable object from its lock tag, primarily used for error reporting and debugging deadlock situations.

## Definition
```c
void DescribeLockTag(StringInfo buf, const LOCKTAG *tag)
```

## Detailed Description
This function appends a human-readable description of a lockable object to a StringInfo buffer based on the provided LOCKTAG. It uses a switch statement to handle different lock tag types and formats appropriate descriptions for each type. The function is designed to avoid acquiring additional locks on system tables (which could cause problems during deadlock reporting) by using only the numeric identifiers stored in the lock tag fields. The descriptions are internationalized using the _() macro for translation support.

## Parameters / Member Variables
- `buf`: StringInfo buffer where the description will be appended
- `tag`: Pointer to the LOCKTAG structure containing the lock information to describe

## Dependencies
- Functions called/Symbols referenced:
  - [appendStringInfo](../a/appendStringInfo.md) (formats and appends text to the buffer)
  - [LockTagType](../L/LockTagType.md) enumeration values (LOCKTAG_RELATION, LOCKTAG_TUPLE, etc.)
  - Internationalization macro _() for translatable strings
- Called from (representative examples):
  - [DeadLockReport](DeadLockReport.md) (for deadlock error reporting)
  - [ProcSleep](../P/ProcSleep.md) (when logging lock wait information)

## Notes and Other Information
- Handles all major lock tag types including relations, pages, tuples, transactions, virtual transactions, speculative tokens, database objects, user locks, advisory locks, and apply transactions
- Uses locktag_field1 through locktag_field4 to extract relevant identifiers based on lock type
- Designed to be safe during deadlock detection when acquiring additional locks could be problematic
- Each lock type has a specific format showing the most relevant identifiers (database ID, relation ID, page number, tuple coordinates, etc.)
- The LOCKTAG_USERLOCK case includes a comment noting it's reserved for old contrib code
- Includes a default case for unrecognized lock tag types to ensure robust error handling

## Simplified Source

```c
void
DescribeLockTag(StringInfo buf, const LOCKTAG *tag)
{
    switch ((LockTagType) tag->locktag_type) {
        case LOCKTAG_RELATION:
            appendStringInfo(buf, _("relation %u of database %u"),
                           tag->locktag_field2, tag->locktag_field1);
            break;

        case LOCKTAG_RELATION_EXTEND:
            appendStringInfo(buf, _("extension of relation %u of database %u"),
                           tag->locktag_field2, tag->locktag_field1);
            break;

        case LOCKTAG_DATABASE_FROZEN_IDS:
            appendStringInfo(buf, _("pg_database.datfrozenxid of database %u"),
                           tag->locktag_field1);
            break;

        case LOCKTAG_PAGE:
            appendStringInfo(buf, _("page %u of relation %u of database %u"),
                           tag->locktag_field3, tag->locktag_field2, tag->locktag_field1);
            break;

        case LOCKTAG_TUPLE:
            appendStringInfo(buf, _("tuple (%u,%u) of relation %u of database %u"),
                           tag->locktag_field3, tag->locktag_field4,
                           tag->locktag_field2, tag->locktag_field1);
            break;

        case LOCKTAG_TRANSACTION:
            appendStringInfo(buf, _("transaction %u"), tag->locktag_field1);
            break;

        case LOCKTAG_VIRTUALTRANSACTION:
            appendStringInfo(buf, _("virtual transaction %d/%u"),
                           tag->locktag_field1, tag->locktag_field2);
            break;

        case LOCKTAG_SPECULATIVE_TOKEN:
            appendStringInfo(buf, _("speculative token %u of transaction %u"),
                           tag->locktag_field2, tag->locktag_field1);
            break;

        case LOCKTAG_OBJECT:
            appendStringInfo(buf, _("object %u of class %u of database %u"),
                           tag->locktag_field3, tag->locktag_field2, tag->locktag_field1);
            break;

        case LOCKTAG_USERLOCK:
            appendStringInfo(buf, _("user lock [%u,%u,%u]"),
                           tag->locktag_field1, tag->locktag_field2, tag->locktag_field3);
            break;

        case LOCKTAG_ADVISORY:
            appendStringInfo(buf, _("advisory lock [%u,%u,%u,%u]"),
                           tag->locktag_field1, tag->locktag_field2,
                           tag->locktag_field3, tag->locktag_field4);
            break;

        case LOCKTAG_APPLY_TRANSACTION:
            appendStringInfo(buf, _("remote transaction %u of subscription %u of database %u"),
                           tag->locktag_field3, tag->locktag_field2, tag->locktag_field1);
            break;

        default:
            appendStringInfo(buf, _("unrecognized locktag type %d"),
                           (int) tag->locktag_type);
            break;
    }
}
```