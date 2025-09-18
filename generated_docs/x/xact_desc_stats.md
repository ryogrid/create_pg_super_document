# xact_desc_stats

## Location
src/backend/access/rmgrdesc/xactdesc.c: 314 - 332

## Overview
A static utility function that formats dropped statistics information from WAL transaction records into human-readable descriptions for debugging and logging purposes.

## Definition


## Detailed Description
This function appends formatted information about dropped statistics to a StringInfo buffer. It iterates through an array of dropped statistics items and formats each one with its kind, database OID, and object OID. The function is used internally by transaction description functions to provide detailed information about statistics that were dropped during transaction operations like commit, abort, or prepare.

## Parameters / Member Variables
- : StringInfo buffer to append the formatted description to
- : String label prefix for the dropped stats description (e.g., "" for normal, "sub" for subtransaction)
- : Number of dropped statistics items in the array
- : Array of xl_xact_stats_item structures containing the dropped statistics information

## Dependencies
- Functions called/Symbols referenced:
  - xl_xact_stats_item (struct type)
  - appendStringInfo (for formatting output)
- Called from (representative examples):
  - xact_desc_commit (src/backend/access/rmgrdesc/xactdesc.c:347)
  - xact_desc_abort (src/backend/access/rmgrdesc/xactdesc.c:392)
  - xact_desc_prepare (src/backend/access/rmgrdesc/xactdesc.c:408-409)

## Notes and Other Information
- This is a static function, only visible within the xactdesc.c file
- Only produces output when ndropped > 0, otherwise does nothing
- Each dropped stats item is formatted as "kind/dboid/objoid" triplet
- Used consistently across different transaction description functions to maintain uniform formatting of dropped statistics information