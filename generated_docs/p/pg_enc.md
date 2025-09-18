# pg_enc

## Location
[src/include/mb/pg_wchar.h:289-290](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/mb/pg_wchar.h#L289-L290)

## Overview
pg_enc is an enumeration type that defines PostgreSQL's encoding identifiers for character set conversions and database encoding support.

## Definition


## Detailed Description
The pg_enc enumeration serves as the central registry for all character encodings supported by PostgreSQL. It provides unique integer identifiers for each supported encoding, enabling consistent encoding identification throughout the PostgreSQL system. The enumeration is carefully organized with PG_SQL_ASCII as the default encoding (value 0), followed by backend-supported encodings, and finally client-only encodings. This structure supports both server-side and client-side character set conversions, ensuring proper text handling across different locales and character sets.

## Parameters / Member Variables
### Backend Encodings (can be used as server encoding):
- : Default SQL/ASCII encoding (value 0)
- : Extended Unix Code for Japanese
- : Extended Unix Code for Chinese
- : Extended Unix Code for Korean
- : Extended Unix Code for Taiwan
- : EUC-JIS-2004 Japanese encoding
- : Unicode UTF-8 encoding
- : Mule internal code
-  through : ISO-8859 Latin character sets
- , , , : Windows code pages
- , , , , , , : Windows code pages
- , : KOI8 Russian and Ukrainian encodings
-  through : Additional ISO-8859 encodings

### Client-Only Encodings:
- : Shift JIS (Windows-932) for Japanese
- : Big5 (Windows-950) for Traditional Chinese
- : GBK (Windows-936) for Simplified Chinese
- : UHC (Windows-949) for Korean
- : GB18030 Chinese encoding
- : EUC for Korean JOHAB
- : Shift-JIS-2004 Japanese encoding
- : Sentinel value marking the end of the enumeration

## Dependencies
- Functions called/Symbols referenced:
  - None (this is a type definition)
- Called from (representative examples):
  - pg_encname
  - [pg_enc2name](pg_enc2name.md)
  - encoding_match
  - [stemmer_module](../s/stemmer_module.md)
  - [xml_out_internal](../x/xml_out_internal.md)

## Notes and Other Information
- PG_SQL_ASCII must always be 0 as it serves as the default encoding
- When adding new encodings, developers must update pg_enc2name_tbl[], pg_enc2gettext_tbl[], and pg_wchar_table[] arrays
- [Backend](../B/Backend.md) encoding IDs are part of libpq's ABI and should not be renumbered for compatibility
- The enumeration distinguishes between backend encodings (can be used as server encoding) and client-only encodings
- PG_ENCODING_BE_LAST macro points to the last backend encoding (PG_KOI8U)
- Client-only encodings cannot be used as server encodings but are available for client-server communication