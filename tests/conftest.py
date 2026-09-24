# Make `import common` resolve to src/common, matching how notebooks add src/ to sys.path.
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))
