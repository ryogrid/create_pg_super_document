# hek2cstr

## Location
[src/pl/plperl/plperl.c:323-379](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plperl/plperl.c#L323-L379)

## Overview
A static function that converts a Perl hash entry (HE) key to a C string in the current database encoding, handling Unicode and character encoding complexities.

## Definition


## Detailed Description
This function converts a Perl hash entry key to a properly encoded C string that matches the current database encoding. It addresses the "Unicode Bug" in Perl where characters in the 128-255 range may not have the UTF8 flag set correctly, but Perl still treats them as Unicode code points. The function uses HeSVKEY_force to create a temporary mortal SV and then applies proper UTF8 handling before converting to a C string via sv2cstr. The function carefully manages memory with ENTER/SAVETMPS/FREETMPS/LEAVE to handle the temporary SV properly.

## Parameters / Member Variables
- : Pointer to a Perl hash entry (HE) whose key needs to be converted to a C string

## Dependencies
- Functions called/Symbols referenced:
  - dTHX (Perl macro for setting up interpreter context)
  - HeSVKEY_force (Perl macro to get SV from hash entry key)
  - HeUTF8 (Perl macro to check UTF8 flag on hash entry)
  - SvUTF8_on (Perl macro to set UTF8 flag on SV)
  - [sv2cstr](../s/sv2cstr.md) (PostgreSQL function to convert SV to C string)
  - ENTER/SAVETMPS/FREETMPS/LEAVE (Perl memory management macros)
- Called from (representative examples):
  - [plperl_build_tuple_result](../p/plperl_build_tuple_result.md)
  - [plperl_modify_tuple](../p/plperl_modify_tuple.md)

## Notes and Other Information
- This function specifically addresses the "Unicode Bug" in Perl where characters 128-255 may not have correct UTF8 flags
- The function ensures proper character encoding conversion between Perl's internal representation and PostgreSQL's database encoding
- Uses Perl's temporary variable management system to safely handle the mortal SV created by HeSVKEY_force
- Critical for proper handling of Unicode column names and hash keys in PL/Perl
- Located in src/pl/plperl/plperl.c at lines 323-379