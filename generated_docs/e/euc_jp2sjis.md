# euc_jp2sjis

## Location
[src/backend/utils/mb/conversion_procs/euc_jp_and_sjis/euc_jp_and_sjis.c:534-637](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conversion_procs/euc_jp_and_sjis/euc_jp_and_sjis.c#L534-L637)

## Overview
Converts text from EUC-JP (Extended Unix Code for Japanese) encoding to Shift JIS (SJIS) encoding, handling various Japanese character sets including ASCII, JIS X0201 kana, JIS X0208 kanji, JIS X0212 kanji, and user-defined characters.

## Definition
static int euc_jp2sjis(const unsigned char *euc, unsigned char *p, int len, bool noError)

## Detailed Description
This function performs character-by-character conversion from EUC-JP to Shift JIS encoding. It processes different types of Japanese characters:

- **ASCII characters**: Copied directly without conversion
- **JIS X0201 half-width katakana**: Identified by SS2 prefix, converted by removing the SS2 prefix
- **JIS X0208 kanji**: Main Japanese character set, converted using standard EUC to SJIS mathematical transformation
- **JIS X0212 kanji**: Extended character set identified by SS3 prefix, handled through lookup tables or UDC2 mapping
- **User-defined characters (UDC1/UDC2)**: Custom character ranges mapped to specific SJIS code points
- **IBM extended kanji**: Special IBM character extensions handled via lookup table

The function includes comprehensive error handling and can operate in two modes: strict (reports encoding errors) or lenient (stops at first invalid sequence).

## Parameters / Member Variables
- : Source buffer containing EUC-JP encoded text to convert
- : Destination buffer where the converted SJIS text will be written
- : Number of bytes remaining in the source buffer to process
- : If true, stops conversion at first invalid sequence; if false, reports encoding errors via report_invalid_encoding

## Dependencies
- Functions called/Symbols referenced:
  - IS_HIGHBIT_SET
  - [report_invalid_encoding](../r/report_invalid_encoding.md)
  - [pg_encoding_verifymbchar](../p/pg_encoding_verifymbchar.md)
  - PG_EUC_JP
  - SS2 (Single Shift 2 - JIS X0201 kana prefix)
  - SS3 (Single Shift 3 - JIS X0212 kanji prefix)  
  - PGSJISALTCODE (alternative character code for unmappable characters)
  - ibmkanji (lookup table for IBM extended characters)
- Called from (representative examples):
  - [euc_jp_to_sjis](euc_jp_to_sjis.md) (public conversion function)

## Notes and Other Information
- The function null-terminates the output buffer
- Returns the number of bytes processed from the input buffer
- Handles multi-byte character boundary validation using pg_encoding_verifymbchar
- Uses mathematical formulas for standard JIS X0208 character conversion: ((c1 - 0xa1) >> 1) + ((c1 < 0xdf) ? 0x81 : 0xc1)
- Special handling for User Defined Character areas (UDC1: 0xf5a1+ in JIS X0208, UDC2: 0xf5a1+ in JIS X0212)
- IBM kanji characters are mapped through a lookup table (ibmkanji array) with fallback to PGSJISALTCODE for unmappable characters
- The conversion process preserves character boundaries and validates input encoding integrity