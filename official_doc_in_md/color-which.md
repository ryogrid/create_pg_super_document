N.2. Configuring the Colors  
---  
[Prev](color-when.md "N.1. When Color is Used") | [Up](color.md "Appendix N. Color Support")| Appendix N. Color Support| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](appendix-obsolete.md "Appendix O. Obsolete or Renamed Features")  
  
* * *

## N.2. Configuring the Colors #

The actual colors to be used are configured using the environment variable `PG_COLORS` (note plural). The value is a colon-separated list of `_`key`_ =_`value`_` pairs. The keys specify what the color is to be used for. The values are SGR (Select Graphic Rendition) specifications, which are interpreted by the terminal. 

The following keys are currently in use: 

`error`
    

used to highlight the text “error” in error messages

`warning`
    

used to highlight the text “warning” in warning messages

`note`
    

used to highlight the text “detail” and “hint” in such messages

`locus`
    

used to highlight location information (e.g., program name and file name) in messages

The default value is `error=01;31:warning=01;35:note=01;36:locus=01` (`01;31` = bold red, `01;35` = bold magenta, `01;36` = bold cyan, `01` = bold default color). 

### Tip

This color specification format is also used by other software packages such as GCC, GNU coreutils, and GNU grep. 

* * *

[Prev](color-when.md "N.1. When Color is Used") | [Up](color.md "Appendix N. Color Support")|  [Next](appendix-obsolete.md "Appendix O. Obsolete or Renamed Features")  
---|---|---  
N.1. When Color is Used | [Home](index.md "PostgreSQL 17.5 Documentation")|  Appendix O. Obsolete or Renamed Features
