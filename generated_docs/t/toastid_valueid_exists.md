# toastid_valueid_exists

## Location
src/backend/access/common/toast_internals.c: 509 - 529

## Overview
Tests whether a toast value with a given ID exists in a toast relation specified by OID, providing a convenient wrapper around toastrel_valueid_exists.

## Definition


## Detailed Description
This internal convenience function provides a simpler interface for checking toast value existence when only the toast relation's OID is available rather than an open Relation structure. It opens the specified toast relation with AccessShareLock, delegates the actual existence check to toastrel_valueid_exists(), and then properly closes the relation before returning the result.

The function maintains the same safety semantics as its underlying implementation, considering both live and dead tuples when determining existence. This approach is particularly useful during table rewrite operations where the caller needs to check for value existence in a toast relation that isn't currently open.

## Parameters / Member Variables
- : The OID of the toast relation to search within
- : The OID of the toast value to search for

## Dependencies
- Functions called/Symbols referenced:
  - table_open
  - table_close
  - toastrel_valueid_exists
- Called from (representative examples):
  - toast_save_datum

## Notes and Other Information
- Declared as static function, internal to toast_internals.c
- Uses AccessShareLock for the toast relation, which is sufficient for read-only existence checking
- Serves as a convenience wrapper when only the toast relation OID is available
- Inherits the safety characteristics of toastrel_valueid_exists regarding live/dead tuple detection
- Properly manages relation lifecycle by opening and closing with matching lock modes
- Used primarily during table rewrite scenarios where OID conflicts need to be avoided across multiple toast relations