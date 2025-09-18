# get_event_trigger_oid

## Location
src/backend/commands/event_trigger.c: 575 - 593

## Overview
Looks up an event trigger by name to find its OID, with optional error handling for missing triggers.

## Definition
```c
Oid get_event_trigger_oid(const char *trigname, bool missing_ok)
```

## Detailed Description
This function performs a catalog lookup to find the OID of an event trigger given its name. It uses the system cache for efficient lookup and provides flexible error handling based on the missing_ok parameter. When the trigger is not found, the function can either throw an error or return InvalidOid depending on the caller's requirements.

## Parameters / Member Variables
- `trigname`: Name of the event trigger to look up
- `missing_ok`: If false, throws an error when trigger not found; if true, returns InvalidOid instead

## Dependencies
- Functions called/Symbols referenced:
  - GetSysCacheOid1 (system cache lookup)
  - CStringGetDatum (string to Datum conversion)
  - EVENTTRIGGERNAME (system cache identifier)
  - OidIsValid (OID validation macro)
  - ereport (error reporting)
- Called from (representative examples):
  - get_object_address_unqualified
  - CALLED_AS_EVENT_TRIGGER (macro context)

## Notes and Other Information
- Uses the EVENTTRIGGERNAME system cache for efficient lookup
- Returns InvalidOid when trigger not found and missing_ok is true
- Throws ERRCODE_UNDEFINED_OBJECT error when trigger not found and missing_ok is false
- Part of the event trigger management API in PostgreSQL
- Commonly used by DDL commands and system functions that need to resolve event trigger names to OIDs