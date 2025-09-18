# mic2big5

## Location
src/backend/utils/mb/conversion_procs/euc_tw_and_big5/euc_tw_and_big5.c: 511 - 580

## Overview
Converts character data from MIC (Mule Internal Code) encoding to Big5 encoding, handling multi-byte character conversion with proper CNS plane recognition and error management.

## Definition
```c
static int mic2big5(const unsigned char *mic, unsigned char *p, int len, bool noError)
```

## Detailed Description
The `mic2big5` function performs character encoding conversion from MIC (Mule Internal Code), PostgreSQL's internal multi-byte encoding format, to Big5 (traditional Chinese character encoding). The function processes MIC-encoded input, handling ASCII characters directly and converting multi-byte MIC characters through CNS 11643 intermediate representation to Big5. It recognizes different CNS planes (1, 2, and private planes 3-4 marked with LCPRV2_B) and extracts the appropriate character codes for conversion. The function includes comprehensive validation of multi-byte character boundaries and provides robust error handling for invalid or untranslatable character sequences.

## Parameters / Member Variables
- `mic`: Input buffer containing MIC encoded data to be converted
- `p`: Output buffer where the converted Big5 encoded data will be written
- `len`: Length of the input data in bytes
- `noError`: Boolean flag controlling error behavior - if true, conversion stops on errors without reporting; if false, errors are reported via PostgreSQL's error system

## Dependencies
- Functions called/Symbols referenced:
  - IS_HIGHBIT_SET (macro for checking if high bit is set)
  - report_invalid_encoding (error reporting for invalid byte sequences)
  - pg_encoding_verifymbchar (validates multi-byte character boundaries)
  - CNStoBIG5 (converts CNS 11643 character codes to Big5 representation)
  - report_untranslatable_char (error reporting for untranslatable characters)
  - PG_MULE_INTERNAL, PG_BIG5 (encoding constants)
  - LC_CNS11643_1, LC_CNS11643_2, LCPRV2_B (character set plane constants)
- Called from:
  - mic_to_big5 (main MIC to Big5 conversion function)

## Notes and Other Information
The function implements a two-step conversion process: MIC → CNS 11643 → Big5, using the CNStoBIG5 lookup function for character mapping. ASCII characters are handled directly without conversion. The function properly distinguishes between standard CNS planes (1 and 2) and private planes (3 and 4), with private planes being identified by the LCPRV2_B prefix byte. Character extraction differs for private planes, where the plane number is in the second byte and character data starts from the third byte. The function integrates with PostgreSQL's encoding verification and error reporting systems, ensuring robust handling of malformed input. Returns the number of input bytes successfully processed, enabling proper handling of partial conversions in streaming scenarios.