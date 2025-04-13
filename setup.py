from setuptools import setup, find_packages


def load_requirements(filename="requirements.txt"):
    with open(filename, encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip() and not line.startswith("#")]


setup(
    name="catin",
    version="0.1.0",
    author="Kamichanw",
    license="MIT",
    description="A Python task scheduling framework.",
    packages=find_packages(),
    include_package_data=True,
    install_requires=load_requirements(),
    entry_points={
        "console_scripts": [
            "meow=catin.cli:main",
        ],
    },
)
