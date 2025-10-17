# puttzcodepass

## Location
[src/timezone/zic.c:2023-2036](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/zic.c#L2023-L2036)

## Overview
A static utility function that conditionally writes integer values to timezone files in either 32-bit or 64-bit format based on the compilation pass number.

## Definition
```c
static void puttzcodepass(zic_t val, FILE *fp, int pass)
```

## Detailed Description
The `puttzcodepass` function provides dual-mode timezone data writing capability. It determines whether to write a value as a 32-bit or 64-bit integer based on the `pass` parameter. When `pass` equals 1, it writes the value as a 32-bit integer using `puttzcode`. For any other pass value, it writes the value as a 64-bit integer using `convert64` and direct file writing. This dual-mode approach allows the timezone compiler to generate different format versions of timezone data files.

## Parameters / Member Variables
- `val`: A 64-bit integer value of type `zic_t` to be written to the file
- `fp`: A file pointer where the binary representation will be written
- `pass`: An integer flag that determines the output format (1 = 32-bit, other = 64-bit)

## Dependencies
- Functions called/Symbols referenced:
  - zic_t (type definition)
  - [puttzcode](puttzcode.md) (for 32-bit output)
  - [convert64](../c/convert64.md) (for 64-bit output)
  - fwrite (standard C library function for file writing)
- Called from (representative examples):
  - (Used internally in timezone compilation process)

## Notes and Other Information
- This function bridges the gap between 32-bit and 64-bit timezone data formats
- Pass 1 generates 32-bit format files for backward compatibility
- Other pass values generate 64-bit format files for extended range support
- The function uses an 8-byte buffer for 64-bit output operations
- Part of PostgreSQL's timezone data compilation infrastructure
- This function is static and only accessible within the zic.c compilation unit

## Simplified Source

```c
static void puttzcodepass(zic_t val, FILE *fp, int pass) {
    // Write timezone data in format determined by pass number
    if (pass == 1) {
        // Pass 1: Write as 32-bit integer for compatibility
        puttzcode(val, fp);
    } else {
        // Other passes: Write as 64-bit integer for extended range
        char buf[8];
        convert64(val, buf);  // Convert to 64-bit binary format
        fwrite(buf, sizeof buf, 1, fp);  // Write to file
    }
}
```