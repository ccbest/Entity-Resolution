"""Package build script"""
import os
import re
import setuptools

ver_file = f'entity_resolution{os.sep}_version.py'
__version__ = None

# Pull package version number from _version.py
with open(ver_file, 'r') as f:
    for line in f.readlines():
        if re.match(r'^\s*#', line):  # comment
            continue

        ver_line = line
        verstr = re.match(r"^.*=\s+'(v\d+\.\d+\.\d+(?:\.[a-zA-Z0-9]+)?)'", ver_line)
        if verstr is not None and len(verstr.groups()) == 1:
            __version__ = verstr.groups()[0]
            break

    if __version__ is None:
        raise EnvironmentError(f'Could not find valid version number in {ver_file}; aborting setup')

with open("./README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="entity-resolution",
    version=__version__,
    author="Carl Best",
    author_email="",
    description="Extensible entity resolution framework",
    long_description=long_description,
    long_description_content_type='text/markdown',
    url="",
    packages=setuptools.find_packages(
        include=('resolver', ),
        exclude=('*tests', 'tests*')
    ),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent"
    ],
    python_requires='>=3.6',
    install_requires=[
        'Levenshtein>=0.18.0,<0.19',
        'networkx>=2.7,<3.0',
        'numpy>=1.22,<2.0',
        'pandas>=1.3.0,<2.0',
        'scikit-learn>=1.0.0',
        'scipy>=1.8,<2.0'
    ],
)
