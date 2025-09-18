# verify_dictoptions

## Location
[src/backend/commands/tsearchcmds.c:342-396](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tsearchcmds.c#L342-L396)

## Overview
This function validates that a text search template's initialization method accepts a proposed set of dictionary options by actually calling the init method.

## Definition
```c
static void verify_dictoptions(Oid tmplId, List *dictoptions)
```

## Detailed Description
The function performs validation of dictionary options against a text search template's capabilities. It first looks up the template in pg_ts_template to find its initialization method. If no init method exists, it rejects any options. If an init method exists, it makes a copy of the options list and calls the init method to verify the options are acceptable.

A special case exists for standalone backend mode (during initdb) where validation is skipped to allow creation of dictionaries that might not be usable in template1's encoding but could be useful in other databases with different encodings.

## Parameters / Member Variables
- `tmplId`: OID of the text search template to validate options against
- `dictoptions`: List of options to validate (DefElem structures)

## Dependencies
- Functions called/Symbols referenced:
  - IsUnderPostmaster: Checks if running under postmaster (not standalone backend)
  - [SearchSysCache1](../S/SearchSysCache1.md): Looks up template information in system cache
  - HeapTupleIsValid: Validates cache lookup result
  - Form_pg_ts_template: Type cast to access template tuple fields
  - OidIsValid: Checks if template has an init method
  - copyObject: Creates deep copy of options list
  - OidFunctionCall1: Calls the template's init method with options
  - [ReleaseSysCache](../R/ReleaseSysCache.md): Releases system cache tuple
- Called from (representative examples):
  - [DefineTSDictionary](../D/DefineTSDictionary.md): Validates options during dictionary creation
  - [AlterTSDictionary](../A/AlterTSDictionary.md): Validates new options during dictionary alteration

## Notes and Other Information
- This is a static function, only accessible within tsearchcmds.c
- Validation is skipped during initdb (standalone backend mode) to allow creation of potentially unusable but future-useful dictionaries
- Options are copied before passing to init method to prevent modification
- Init method is expected to throw an error if options are invalid
- Memory leaks from init method calls are not a concern since command execution ends soon after
- Templates without init methods cannot accept any options
- Function does not return a value; validation failure results in an error being thrown