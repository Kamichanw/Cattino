from setuptools import setup, find_packages
from pathlib import Path

ROOT_DIR = Path(__file__).parent


def get_requirements() -> list[str]:
    """Get Python package dependencies from requirements.txt."""
    requirements_dir = ROOT_DIR / "requirements"

    def _read_requirements(filename: str) -> list[str]:
        with open(requirements_dir / filename) as f:
            requirements = f.read().strip().split("\n")
        resolved_requirements = []
        for line in requirements:
            if line.startswith("-r "):
                resolved_requirements += _read_requirements(line.split()[1])
            elif (
                not line.startswith("--")
                and not line.startswith("#")
                and line.strip() != ""
            ):
                resolved_requirements.append(line)
        return resolved_requirements

    return _read_requirements("common.txt")


setup(
    name="cattino",
    version="0.1.0",
    author="Kamichanw",
    license="MIT",
    description="A Python task scheduling framework.",
    packages=find_packages(),
    include_package_data=True,
    install_requires=get_requirements(),
    entry_points={
        "console_scripts": [
            "meow=cattino.cli:main",
        ],
    },
)
