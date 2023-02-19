import setuptools

with open("./README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="entity-resolution",
    version='v0.1.0',
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
