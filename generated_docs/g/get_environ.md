# get_environ

## Location
[src/test/regress/regress.c:651-673](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/regress/regress.c#L651-L673)

## Overview
A PostgreSQL test function that retrieves all environment variables from the current process and returns them as a PostgreSQL text array.

## Definition

```c
struct_array_builtin(env, nvals, TEXTOID);
```
## Detailed Description
The  function is a test utility that provides access to the process environment variables from within PostgreSQL. It accesses the global  variable (a null-terminated array of strings) that contains all environment variables in "KEY=VALUE" format, counts the total number of environment variables, and constructs a PostgreSQL array containing all environment variable strings as text elements. This function is useful for regression testing scenarios where the test environment setup needs to be verified or when testing PostgreSQL's interaction with the operating system environment. The function allocates memory to store all environment variable strings as PostgreSQL Datum values and constructs a proper PostgreSQL array type for return.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro (no specific arguments used by this function)
- : External global variable containing null-terminated array of environment variable strings
- : Counter for the total number of environment variables
- : The constructed PostgreSQL ArrayType containing all environment variables
- SHELL=/bin/bash
COREPACK_ENABLE_AUTO_PIN=0
NVM_INC=/home/ryo/.nvm/versions/node/v22.18.0/include/node
WSL2_GUI_APPS_ENABLED=1
WSL_DISTRO_NAME=Ubuntu-20.04
WT_SESSION=6c0a554d-8c5e-414d-b4cc-37ccd7aec4ea
RBENV_SHELL=bash
NAME=DESKTOP-IOASPN6
PWD=/home/ryo/work/postgres_17_6
LOGNAME=ryo
CLAUDECODE=1
HOME=/home/ryo
LANG=C.UTF-8
WSL_INTEROP=/run/WSL/1966_interop
LS_COLORS=rs=0:di=01;34:ln=01;36:mh=00:pi=40;33:so=01;35:do=01;35:bd=40;33;01:cd=40;33;01:or=40;31;01:mi=00:su=37;41:sg=30;43:ca=30;41:tw=30;42:ow=34;42:st=37;44:ex=01;32:*.tar=01;31:*.tgz=01;31:*.arc=01;31:*.arj=01;31:*.taz=01;31:*.lha=01;31:*.lz4=01;31:*.lzh=01;31:*.lzma=01;31:*.tlz=01;31:*.txz=01;31:*.tzo=01;31:*.t7z=01;31:*.zip=01;31:*.z=01;31:*.dz=01;31:*.gz=01;31:*.lrz=01;31:*.lz=01;31:*.lzo=01;31:*.xz=01;31:*.zst=01;31:*.tzst=01;31:*.bz2=01;31:*.bz=01;31:*.tbz=01;31:*.tbz2=01;31:*.tz=01;31:*.deb=01;31:*.rpm=01;31:*.jar=01;31:*.war=01;31:*.ear=01;31:*.sar=01;31:*.rar=01;31:*.alz=01;31:*.ace=01;31:*.zoo=01;31:*.cpio=01;31:*.7z=01;31:*.rz=01;31:*.cab=01;31:*.wim=01;31:*.swm=01;31:*.dwm=01;31:*.esd=01;31:*.jpg=01;35:*.jpeg=01;35:*.mjpg=01;35:*.mjpeg=01;35:*.gif=01;35:*.bmp=01;35:*.pbm=01;35:*.pgm=01;35:*.ppm=01;35:*.tga=01;35:*.xbm=01;35:*.xpm=01;35:*.tif=01;35:*.tiff=01;35:*.png=01;35:*.svg=01;35:*.svgz=01;35:*.mng=01;35:*.pcx=01;35:*.mov=01;35:*.mpg=01;35:*.mpeg=01;35:*.m2v=01;35:*.mkv=01;35:*.webm=01;35:*.webp=01;35:*.ogm=01;35:*.mp4=01;35:*.m4v=01;35:*.mp4v=01;35:*.vob=01;35:*.qt=01;35:*.nuv=01;35:*.wmv=01;35:*.asf=01;35:*.rm=01;35:*.rmvb=01;35:*.flc=01;35:*.avi=01;35:*.fli=01;35:*.flv=01;35:*.gl=01;35:*.dl=01;35:*.xcf=01;35:*.xwd=01;35:*.yuv=01;35:*.cgm=01;35:*.emf=01;35:*.ogv=01;35:*.ogx=01;35:*.aac=00;36:*.au=00;36:*.flac=00;36:*.m4a=00;36:*.mid=00;36:*.midi=00;36:*.mka=00;36:*.mp3=00;36:*.mpc=00;36:*.ogg=00;36:*.ra=00;36:*.wav=00;36:*.oga=00;36:*.opus=00;36:*.spx=00;36:*.xspf=00;36:
VIRTUAL_ENV=/home/ryo/work/postgres_17_6/venv
WAYLAND_DISPLAY=wayland-0
PERL5LIB=/home/ryo/perl5/lib/perl5
NVM_DIR=/home/ryo/.nvm
LESSCLOSE=/usr/bin/lesspipe %s %s
TERM=xterm-256color
PERL_MB_OPT=--install_base "/home/ryo/perl5"
LESSOPEN=| /usr/bin/lesspipe %s
USER=ryo
PERL_MM_OPT=INSTALL_BASE=/home/ryo/perl5
DISPLAY=:0
SHLVL=1
NVM_CD_FLAGS=
GIT_EDITOR=true
VIRTUAL_ENV_PROMPT=(venv) 
OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE=delta
XDG_RUNTIME_DIR=/mnt/wslg/runtime-dir
CLAUDE_CODE_ENTRYPOINT=sdk-cli
WSLENV=WT_SESSION:WT_PROFILE_ID:
XDG_DATA_DIRS=/usr/local/share:/usr/share:/var/lib/snapd/desktop
PERL_LOCAL_LIB_ROOT=/home/ryo/perl5
PATH=/home/ryo/work/postgres_17_6/venv/bin:/home/ryo/.local/bin:/home/ryo/bin:/home/ryo/.nvm/versions/node/v22.18.0/bin:/home/ryo/perl5/bin:/home/ryo/.cargo/bin:/home/ryo/.rbenv/shims:~/.rbenv/bin:/usr/local/zig:/home/ryo/bin/cov-analysis-linux64-2023.12.2/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/usr/lib/wsl/lib:/mnt/c/Program Files/Alacritty/:/mnt/c/Users/ryo/AppData/Local/Programs/cursor/resources/app/bin:/mnt/c/Program Files/NVIDIA GPU Computing Toolkit/CUDA/v12.0/bin:/mnt/c/Program Files/NVIDIA GPU Computing Toolkit/CUDA/v12.0/libnvvp:/mnt/c/Nvidia/cudnn-windows-x86_64-9.5.0.50_cuda12-archive/bin:/mnt/c/Program Files/Microsoft/jdk-11.0.16.101-hotspot/bin:/mnt/c/Ruby33-x64/bin:/mnt/c/Users/ryo/dev/flutter/bin:/mnt/c/Program Files/NVIDIA GPU Computing Toolkit/CUDA/v11.3/bin:/mnt/c/Program Files/NVIDIA GPU Computing Toolkit/CUDA/v11.3/libnvvp:/mnt/c/Python310/Scripts/:/mnt/c/Python310/:/mnt/c/Program Files/Common Files/Oracle/Java/javapath:/mnt/c/Program Files/Oculus/Support/oculus-runtime:/mnt/c/Windows/system32:/mnt/c/Windows:/mnt/c/Windows/System32/Wbem:/mnt/c/Windows/System32/WindowsPowerShell/v1.0/:/mnt/c/Windows/System32/OpenSSH/:/mnt/c/WINDOWS/system32:/mnt/c/WINDOWS:/mnt/c/WINDOWS/System32/Wbem:/mnt/c/WINDOWS/System32/WindowsPowerShell/v1.0/:/mnt/c/WINDOWS/System32/OpenSSH/:/mnt/c/Program Files (x86)/GtkSharp/2.12/bin:/mnt/c/Program Files/PuTTY/:/mnt/c/Program Files (x86)/NVIDIA Corporation/PhysX/:/mnt/c/Program Files/Git/bin:/mnt/h/program_files_noinst/win_flex_bison-latest:/mnt/c/Program Files/Meson/:/mnt/c/Strawberry/perl/bin:/mnt/h/program_files_noinst/gnuwin32/bin:/mnt/c/WINDOWS/system32:/mnt/c/WINDOWS:/mnt/c/WINDOWS/System32/Wbem:/mnt/c/WINDOWS/System32/WindowsPowerShell/v1.0/:/mnt/c/WINDOWS/System32/OpenSSH/:/mnt/c/Program Files/Tailscale/:/mnt/c/Program Files/Go/bin:/mnt/c/Program Files/Graphviz/bin:/mnt/c/Program Files/WinGet/Links:/mnt/c/Program Files/dotnet/:/mnt/c/Users/ryo/dev/flutter/bin:/mnt/c/Users/ryo/.cargo/bin:/mnt/c/Users/ryo/AppData/Local/Programs/Python/Python37/Scripts/:/mnt/c/Users/ryo/AppData/Local/Programs/Python/Python37/:/mnt/c/Users/ryo/AppData/Local/Microsoft/WindowsApps:/mnt/c/Users/ryo/AppData/Local/Programs/Microsoft VS Code/bin:/mnt/c/Program Files/JetBrains/PyCharm Community Edition 2019.2.5/bin:/mnt/h/ProgramFiles/Fiddler:/mnt/c/Program Files/JetBrains/PyCharm Community Edition 2020.2.3/bin:/mnt/c/Users/ryo/AppData/Local/Programs/IPFS Desktop/resources/app.asar.unpacked/src/ipfs-on-path/scripts/bin-win:/mnt/c/Users/ryo/AppData/Local/Microsoft/WindowsApps:/mnt/c/Program Files (x86)/GitHub CLI/:/mnt/c/Program Files/JetBrains/GoLand 2024.1.4/bin:/mnt/c/Program Files/heroku/bin:/mnt/c/Program Files/JetBrains/CLion 2024.2.1/bin:/mnt/c/Users/ryo/AppData/Roaming/npm:/mnt/c/Users/ryo/AppData/Roaming/nvm:/mnt/c/Program Files/nodejs:/mnt/c/Data/work/algia-web:/mnt/c/Users/ryo/.dotnet/tools:/mnt/c/tools/dart-sdk/bin:/mnt/c/Users/ryo/AppData/Local/Pub/Cache/bin:/mnt/c/Users/ryo/AppData/Local/Programs/Ollama:/mnt/c/Program Files/JetBrains/PyCharm 2024.2.3/bin:/mnt/c/Program Files/JetBrains/JetBrains Gateway 2024.2.3/bin:/mnt/c/Users/ryo/AppData/Local/Programs/cursor/resources/app/bin:/mnt/c/Users/ryo/go/bin:/mnt/c/Users/ryo/.lmstudio/bin:/mnt/c/Program Files/JetBrains/IntelliJ IDEA Community Edition 2025.1/bin:/snap/bin:/home/ryo/.fzf/bin
NVM_BIN=/home/ryo/.nvm/versions/node/v22.18.0/bin
HOSTTYPE=x86_64
PULSE_SERVER=unix:/mnt/wslg/PulseServer
WT_PROFILE_ID={4dd1e689-b517-5f39-947d-78e8a8bdf958}
OLDPWD=/home/ryo/work
_=/usr/bin/env: Array of Datum values representing environment variable strings

## Dependencies
- Functions called/Symbols referenced:
  - : Global environment variable array from the C runtime
  - : PostgreSQL memory allocation function
  - : Converts C-string to PostgreSQL text Datum
  - : Creates PostgreSQL array from Datum values
  - : Macro to return pointer value from PostgreSQL function
  - : PostgreSQL text type OID constant
- Called from (representative examples):
  - : Referenced in the same test regression file

## Notes and Other Information
- This is a test function located in the PostgreSQL regression test suite
- The function takes no input parameters and operates on the global process environment
- Each environment variable is returned as a complete "KEY=VALUE" string
- The function counts environment variables twice: once to determine array size, once to populate the array
- Uses PostgreSQL's built-in array construction functions to create a proper text array
- The returned array contains all environment variables visible to the PostgreSQL process
- Useful for testing environment variable access and array construction functionality
- The function follows PostgreSQL's V1 calling convention for user-defined functions