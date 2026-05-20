#!/usr/bin/env python3

import os
import sys


def _bootstrap() -> None:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.dirname(script_dir)
    agent_dir = os.path.join(repo_root, "Agent")
    if agent_dir not in sys.path:
        sys.path.insert(0, agent_dir)


_bootstrap()

from vitabench_delivery import main  # noqa: E402


if __name__ == "__main__":
    sys.exit(main())
