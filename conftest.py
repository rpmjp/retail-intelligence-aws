"""Pytest configuration: set JAVA_HOME for PySpark before any tests collect."""

import os

os.environ.setdefault("JAVA_HOME", "/usr/lib/jvm/java-17-openjdk-amd64")
