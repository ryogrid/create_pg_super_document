# ResourceOwnerRememberWaitEventSet

## Location
[src/backend/storage/ipc/latch.c:214-218](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/latch.c#L214-L218)

## Overview
A convenience wrapper function that registers a WaitEventSet with a ResourceOwner for automatic cleanup when the resource owner is released.

## Definition

```c
static inline void
ResourceOwnerRememberWaitEventSet(ResourceOwner owner, WaitEventSet *set)
```
## Detailed Description
This function serves as a convenience wrapper around ResourceOwnerRemember, specifically designed for managing WaitEventSet resources. It registers a WaitEventSet with a ResourceOwner using the predefined wait_event_set_resowner_desc descriptor. This ensures that when the ResourceOwner is released or reset, the associated WaitEventSet will be automatically cleaned up, preventing resource leaks. The function is implemented as a static inline function for performance, converting the WaitEventSet pointer to a Datum using PointerGetDatum before passing it to the generic ResourceOwnerRemember function.

## Parameters / Member Variables
- : The ResourceOwner that will track and manage the WaitEventSet resource
- BASH=/bin/bash
BASHOPTS=checkwinsize:cmdhist:complete_fullquote:expand_aliases:extquote:force_fignore:globasciiranges:hostcomplete:interactive_comments:login_shell:progcomp:promptvars:sourcepath
BASH_ALIASES=()
BASH_ARGC=([0]="0")
BASH_ARGV=()
BASH_CMDS=()
BASH_COMPAT=51
BASH_EXECUTION_STRING=$'source /home/ryo/.claude/shell-snapshots/snapshot-bash-1758651133613-q06se8.sh && eval \'python3 scripts/mcp_tool.py return_document ResourceOwnerRememberWaitEventSet "# ResourceOwnerRememberWaitEventSet\n\n## Overview\nA convenience wrapper function that registers a WaitEventSet with a ResourceOwner for automatic cleanup when the resource owner is released.\n\n## Definition\n```c\nstatic inline void\nResourceOwnerRememberWaitEventSet(ResourceOwner owner, WaitEventSet *set)\n```\n\n## Detailed Description\nThis function serves as a convenience wrapper around ResourceOwnerRemember, specifically designed for managing WaitEventSet resources. It registers a WaitEventSet with a ResourceOwner using the predefined wait_event_set_resowner_desc descriptor. This ensures that when the ResourceOwner is released or reset, the associated WaitEventSet will be automatically cleaned up, preventing resource leaks. The function is implemented as a static inline function for performance, converting the WaitEventSet pointer to a Datum using PointerGetDatum before passing it to the generic ResourceOwnerRemember function.\n\n## Parameters / Member Variables\n- `owner`: The ResourceOwner that will track and manage the WaitEventSet resource\n- `set`: The WaitEventSet to be registered with the ResourceOwner for automatic cleanup\n\n## Dependencies\n- Functions called/Symbols referenced:\n  - [ResourceOwnerRemember](ResourceOwnerRemember.md)\n  - [PointerGetDatum](../P/PointerGetDatum.md)\n  - wait_event_set_resowner_desc (descriptor)\n- Called from (representative examples):\n  - [CreateWaitEventSet](../C/CreateWaitEventSet.md)\n\n## Notes and Other Information\n- This is a static inline function located in src/backend/storage/ipc/latch.c\n- Part of PostgreSQL\'"\'"\'s resource management system that prevents resource leaks\n- Works in conjunction with ResourceOwnerForgetWaitEventSet for complete lifecycle management\n- The function uses a predefined resource descriptor (wait_event_set_resowner_desc) to specify cleanup behavior"\' < /dev/null && pwd -P >| /tmp/claude-54eb-cwd'
BASH_LINENO=()
BASH_SOURCE=()
BASH_VERSINFO=([0]="5" [1]="1" [2]="16" [3]="1" [4]="release" [5]="x86_64-pc-linux-gnu")
BASH_VERSION='5.1.16(1)-release'
CLAUDECODE=1
CLAUDE_CODE_ENTRYPOINT=sdk-cli
COREPACK_ENABLE_AUTO_PIN=0
DIRSTACK=()
DISPLAY=:0
EUID=1000
GIT_EDITOR=true
GROUPS=()
HOME=/home/ryo
HOSTNAME=DESKTOP-IOASPN6
HOSTTYPE=x86_64
IFS=$' \t\n'
LANG=C.UTF-8
LESSCLOSE='/usr/bin/lesspipe %s %s'
LESSOPEN='| /usr/bin/lesspipe %s'
LOGNAME=ryo
LS_COLORS='rs=0:di=01;34:ln=01;36:mh=00:pi=40;33:so=01;35:do=01;35:bd=40;33;01:cd=40;33;01:or=40;31;01:mi=00:su=37;41:sg=30;43:ca=30;41:tw=30;42:ow=34;42:st=37;44:ex=01;32:*.tar=01;31:*.tgz=01;31:*.arc=01;31:*.arj=01;31:*.taz=01;31:*.lha=01;31:*.lz4=01;31:*.lzh=01;31:*.lzma=01;31:*.tlz=01;31:*.txz=01;31:*.tzo=01;31:*.t7z=01;31:*.zip=01;31:*.z=01;31:*.dz=01;31:*.gz=01;31:*.lrz=01;31:*.lz=01;31:*.lzo=01;31:*.xz=01;31:*.zst=01;31:*.tzst=01;31:*.bz2=01;31:*.bz=01;31:*.tbz=01;31:*.tbz2=01;31:*.tz=01;31:*.deb=01;31:*.rpm=01;31:*.jar=01;31:*.war=01;31:*.ear=01;31:*.sar=01;31:*.rar=01;31:*.alz=01;31:*.ace=01;31:*.zoo=01;31:*.cpio=01;31:*.7z=01;31:*.rz=01;31:*.cab=01;31:*.wim=01;31:*.swm=01;31:*.dwm=01;31:*.esd=01;31:*.jpg=01;35:*.jpeg=01;35:*.mjpg=01;35:*.mjpeg=01;35:*.gif=01;35:*.bmp=01;35:*.pbm=01;35:*.pgm=01;35:*.ppm=01;35:*.tga=01;35:*.xbm=01;35:*.xpm=01;35:*.tif=01;35:*.tiff=01;35:*.png=01;35:*.svg=01;35:*.svgz=01;35:*.mng=01;35:*.pcx=01;35:*.mov=01;35:*.mpg=01;35:*.mpeg=01;35:*.m2v=01;35:*.mkv=01;35:*.webm=01;35:*.webp=01;35:*.ogm=01;35:*.mp4=01;35:*.m4v=01;35:*.mp4v=01;35:*.vob=01;35:*.qt=01;35:*.nuv=01;35:*.wmv=01;35:*.asf=01;35:*.rm=01;35:*.rmvb=01;35:*.flc=01;35:*.avi=01;35:*.fli=01;35:*.flv=01;35:*.gl=01;35:*.dl=01;35:*.xcf=01;35:*.xwd=01;35:*.yuv=01;35:*.cgm=01;35:*.emf=01;35:*.ogv=01;35:*.ogx=01;35:*.aac=00;36:*.au=00;36:*.flac=00;36:*.m4a=00;36:*.mid=00;36:*.midi=00;36:*.mka=00;36:*.mp3=00;36:*.mpc=00;36:*.ogg=00;36:*.ra=00;36:*.wav=00;36:*.oga=00;36:*.opus=00;36:*.spx=00;36:*.xspf=00;36:'
MACHTYPE=x86_64-pc-linux-gnu
NAME=DESKTOP-IOASPN6
NVM_BIN=/home/ryo/.nvm/versions/node/v22.18.0/bin
NVM_CD_FLAGS=
NVM_DIR=/home/ryo/.nvm
NVM_INC=/home/ryo/.nvm/versions/node/v22.18.0/include/node
NoDefaultCurrentDirectoryInExePath=1
OLDPWD=/home/ryo/work/postgres_17_6_sub/any-script-mcp-repo
OPTERR=1
OPTIND=1
OSTYPE=linux-gnu
OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE=delta
PATH='/home/ryo/work/postgres_17_6/venv/bin:/home/ryo/.local/bin:/home/ryo/bin:/home/ryo/.nvm/versions/node/v22.18.0/bin:/home/ryo/perl5/bin:/home/ryo/.cargo/bin:/home/ryo/.rbenv/shims:~/.rbenv/bin:/usr/local/zig:/home/ryo/bin/cov-analysis-linux64-2023.12.2/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/usr/lib/wsl/lib:/mnt/c/Program Files/Alacritty/:/mnt/c/Users/ryo/AppData/Local/Programs/cursor/resources/app/bin:/mnt/c/Program Files/NVIDIA GPU Computing Toolkit/CUDA/v12.0/bin:/mnt/c/Program Files/NVIDIA GPU Computing Toolkit/CUDA/v12.0/libnvvp:/mnt/c/Nvidia/cudnn-windows-x86_64-9.5.0.50_cuda12-archive/bin:/mnt/c/Program Files/Microsoft/jdk-11.0.16.101-hotspot/bin:/mnt/c/Ruby33-x64/bin:/mnt/c/Users/ryo/dev/flutter/bin:/mnt/c/Program Files/NVIDIA GPU Computing Toolkit/CUDA/v11.3/bin:/mnt/c/Program Files/NVIDIA GPU Computing Toolkit/CUDA/v11.3/libnvvp:/mnt/c/Python310/Scripts/:/mnt/c/Python310/:/mnt/c/Program Files/Common Files/Oracle/Java/javapath:/mnt/c/Program Files/Oculus/Support/oculus-runtime:/mnt/c/Windows/system32:/mnt/c/Windows:/mnt/c/Windows/System32/Wbem:/mnt/c/Windows/System32/WindowsPowerShell/v1.0/:/mnt/c/Windows/System32/OpenSSH/:/mnt/c/WINDOWS/system32:/mnt/c/WINDOWS:/mnt/c/WINDOWS/System32/Wbem:/mnt/c/WINDOWS/System32/WindowsPowerShell/v1.0/:/mnt/c/WINDOWS/System32/OpenSSH/:/mnt/c/Program Files (x86)/GtkSharp/2.12/bin:/mnt/c/Program Files/PuTTY/:/mnt/c/Program Files (x86)/NVIDIA Corporation/PhysX/:/mnt/c/Program Files/Git/bin:/mnt/h/program_files_noinst/win_flex_bison-latest:/mnt/c/Program Files/Meson/:/mnt/c/Strawberry/perl/bin:/mnt/h/program_files_noinst/gnuwin32/bin:/mnt/c/WINDOWS/system32:/mnt/c/WINDOWS:/mnt/c/WINDOWS/System32/Wbem:/mnt/c/WINDOWS/System32/WindowsPowerShell/v1.0/:/mnt/c/WINDOWS/System32/OpenSSH/:/mnt/c/Program Files/Tailscale/:/mnt/c/Program Files/Go/bin:/mnt/c/Program Files/Graphviz/bin:/mnt/c/Program Files/WinGet/Links:/mnt/c/Program Files/dotnet/:/mnt/c/Users/ryo/dev/flutter/bin:/mnt/c/Users/ryo/.cargo/bin:/mnt/c/Users/ryo/AppData/Local/Programs/Python/Python37/Scripts/:/mnt/c/Users/ryo/AppData/Local/Programs/Python/Python37/:/mnt/c/Users/ryo/AppData/Local/Microsoft/WindowsApps:/mnt/c/Users/ryo/AppData/Local/Programs/Microsoft VS Code/bin:/mnt/c/Program Files/JetBrains/PyCharm Community Edition 2019.2.5/bin:/mnt/h/ProgramFiles/Fiddler:/mnt/c/Program Files/JetBrains/PyCharm Community Edition 2020.2.3/bin:/mnt/c/Users/ryo/AppData/Local/Programs/IPFS Desktop/resources/app.asar.unpacked/src/ipfs-on-path/scripts/bin-win:/mnt/c/Users/ryo/AppData/Local/Microsoft/WindowsApps:/mnt/c/Program Files (x86)/GitHub CLI/:/mnt/c/Program Files/JetBrains/GoLand 2024.1.4/bin:/mnt/c/Program Files/heroku/bin:/mnt/c/Program Files/JetBrains/CLion 2024.2.1/bin:/mnt/c/Users/ryo/AppData/Roaming/npm:/mnt/c/Users/ryo/AppData/Roaming/nvm:/mnt/c/Program Files/nodejs:/mnt/c/Data/work/algia-web:/mnt/c/Users/ryo/.dotnet/tools:/mnt/c/tools/dart-sdk/bin:/mnt/c/Users/ryo/AppData/Local/Pub/Cache/bin:/mnt/c/Users/ryo/AppData/Local/Programs/Ollama:/mnt/c/Program Files/JetBrains/PyCharm 2024.2.3/bin:/mnt/c/Program Files/JetBrains/JetBrains Gateway 2024.2.3/bin:/mnt/c/Users/ryo/AppData/Local/Programs/cursor/resources/app/bin:/mnt/c/Users/ryo/go/bin:/mnt/c/Users/ryo/.lmstudio/bin:/mnt/c/Program Files/JetBrains/IntelliJ IDEA Community Edition 2025.1/bin:/snap/bin:/home/ryo/.fzf/bin'
PERL5LIB=/home/ryo/perl5/lib/perl5
PERL_LOCAL_LIB_ROOT=/home/ryo/perl5
PERL_MB_OPT='--install_base "/home/ryo/perl5"'
PERL_MM_OPT=INSTALL_BASE=/home/ryo/perl5
PIPESTATUS=([0]="0")
PPID=11002
PS4='+ '
PULSE_SERVER=unix:/mnt/wslg/PulseServer
PWD=/home/ryo/work/postgres_17_6_sub
RBENV_SHELL=bash
SHELL=/bin/bash
SHELLOPTS=braceexpand:hashall:interactive-comments:monitor:onecmd
SHLVL=2
TERM=xterm-256color
UID=1000
USER=ryo
VIRTUAL_ENV=/home/ryo/work/postgres_17_6/venv
VIRTUAL_ENV_PROMPT='(venv) '
WAYLAND_DISPLAY=wayland-0
WSL2_GUI_APPS_ENABLED=1
WSLENV=WT_SESSION:WT_PROFILE_ID:
WSL_DISTRO_NAME=Ubuntu-20.04
WSL_INTEROP=/run/WSL/2189_interop
WT_PROFILE_ID='{4dd1e689-b517-5f39-947d-78e8a8bdf958}'
WT_SESSION=7bf8822b-7977-4a28-a6ad-aab37ffc003e
XDG_DATA_DIRS=/usr/local/share:/usr/share:/var/lib/snapd/desktop
XDG_RUNTIME_DIR=/mnt/wslg/runtime-dir
_=/home/ryo/.claude/shell-snapshots/snapshot-bash-1758651133613-q06se8.sh
snap_bin_path=/snap/bin
snap_xdg_path=/var/lib/snapd/desktop
gawklibpath_append () 
{ 
    [ -z "$AWKLIBPATH" ] && AWKLIBPATH=`gawk 'BEGIN {print ENVIRON["AWKLIBPATH"]}'`;
    export AWKLIBPATH="$AWKLIBPATH:$*"
}
gawklibpath_default () 
{ 
    unset AWKLIBPATH;
    export AWKLIBPATH=`gawk 'BEGIN {print ENVIRON["AWKLIBPATH"]}'`
}
gawklibpath_prepend () 
{ 
    [ -z "$AWKLIBPATH" ] && AWKLIBPATH=`gawk 'BEGIN {print ENVIRON["AWKLIBPATH"]}'`;
    export AWKLIBPATH="$*:$AWKLIBPATH"
}
gawkpath_append () 
{ 
    [ -z "$AWKPATH" ] && AWKPATH=`gawk 'BEGIN {print ENVIRON["AWKPATH"]}'`;
    export AWKPATH="$AWKPATH:$*"
}
gawkpath_default () 
{ 
    unset AWKPATH;
    export AWKPATH=`gawk 'BEGIN {print ENVIRON["AWKPATH"]}'`
}
gawkpath_prepend () 
{ 
    [ -z "$AWKPATH" ] && AWKPATH=`gawk 'BEGIN {print ENVIRON["AWKPATH"]}'`;
    export AWKPATH="$*:$AWKPATH"
}: The WaitEventSet to be registered with the ResourceOwner for automatic cleanup

## Dependencies
- Functions called/Symbols referenced:
  - [ResourceOwnerRemember](ResourceOwnerRemember.md)
  - [PointerGetDatum](../P/PointerGetDatum.md)
  - wait_event_set_resowner_desc (descriptor)
- Called from (representative examples):
  - [CreateWaitEventSet](../C/CreateWaitEventSet.md)

## Notes and Other Information
- This is a static inline function located in src/backend/storage/ipc/latch.c
- Part of PostgreSQL's resource management system that prevents resource leaks
- Works in conjunction with ResourceOwnerForgetWaitEventSet for complete lifecycle management
- The function uses a predefined resource descriptor (wait_event_set_resowner_desc) to specify cleanup behavior