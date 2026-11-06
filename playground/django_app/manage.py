#!/usr/bin/env python
import os
import sys
from pathlib import Path

# shortcut: makes the playground's workflow/steps modules importable; in a real project they live in the app package
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

if __name__ == "__main__":
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "project.settings")
    from django.core.management import execute_from_command_line

    execute_from_command_line(sys.argv)
