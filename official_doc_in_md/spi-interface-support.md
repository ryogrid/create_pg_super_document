45.2. Interface Support Functions  
---  
[Prev](spi-spi-register-trigger-data.md "SPI_register_trigger_data") | [Up](spi.md "Chapter 45. Server Programming Interface")| Chapter 45. Server Programming Interface| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](spi-spi-fname.md "SPI_fname")  
  
* * *

## 45.2. Interface Support Functions #

[SPI_fname](spi-spi-fname.md) — determine the column name for the specified column number
[SPI_fnumber](spi-spi-fnumber.md) — determine the column number for the specified column name
[SPI_getvalue](spi-spi-getvalue.md) — return the string value of the specified column
[SPI_getbinval](spi-spi-getbinval.md) — return the binary value of the specified column
[SPI_gettype](spi-spi-gettype.md) — return the data type name of the specified column
[SPI_gettypeid](spi-spi-gettypeid.md) — return the data type OID of the specified column
[SPI_getrelname](spi-spi-getrelname.md) — return the name of the specified relation
[SPI_getnspname](spi-spi-getnspname.md) — return the namespace of the specified relation
[SPI_result_code_string](spi-spi-result-code-string.md) — return error code as string

The functions described here provide an interface for extracting information from result sets returned by `SPI_execute` and other SPI functions. 

All functions described in this section can be used by both connected and unconnected C functions. 

* * *

[Prev](spi-spi-register-trigger-data.md "SPI_register_trigger_data") | [Up](spi.md "Chapter 45. Server Programming Interface")|  [Next](spi-spi-fname.md "SPI_fname")  
---|---|---  
SPI_register_trigger_data | [Home](index.md "PostgreSQL 17.5 Documentation")|  SPI_fname
