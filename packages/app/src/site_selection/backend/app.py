import sys
from pathlib import Path

# In local dev the legacy modules live at ../../../../../../src/app relative to
# this file.  In a deployed wheel they are bundled as site_selection/_legacy/.
# Try the packaged location first; fall back to the source tree for local dev.
_pkg_legacy = Path(__file__).resolve().parent.parent / "_legacy"
_src_legacy = Path(__file__).resolve().parent.parent.parent.parent.parent.parent / "src" / "app"

for _candidate in (_pkg_legacy, _src_legacy):
    if _candidate.is_dir() and str(_candidate) not in sys.path:
        sys.path.insert(0, str(_candidate))
        break

from .core import create_app
from .router import router

app = create_app(routers=[router])
