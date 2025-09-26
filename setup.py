from setuptools import setup, find_packages
from pathlib import Path

ROOT_DIR = Path(__file__).parent


def get_requirements(extras: dict[str, str] | None = None) -> list[str]:
    """Get Python package dependencies from requirements.txt.

    Handles extras by adding the corresponding requirements files.
    """
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

    base_requirements = _read_requirements("common.txt")

    if extras:
        extra_requirements = []
        for extra_name, extra_files in extras.items():
            for extra_file in extra_files.split(","):
                extra_requirements += _read_requirements(extra_file.strip())
        return base_requirements + extra_requirements
    else:
        return base_requirements


setup(
    name="cattino",
    version="0.1.0",
    author="Kamichanw",
    license="MIT",
    description="A Python task scheduling framework.",
    packages=find_packages(),
    include_package_data=True,
    extras_require={"docs": "docs.txt"},
    install_requires=get_requirements(),
    entry_points={
        "console_scripts": [
            "meow=cattino.cli:main",
        ],
    },
)
