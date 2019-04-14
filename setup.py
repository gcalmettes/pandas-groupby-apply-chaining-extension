import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="gcGroupbyExtension-gcalmettes",
    version="0.0.3",
    author="Guillaume Calmettes",
    author_email="gcalmettes@g.ucla.edu",
    description="Allows to construct a pipeline of functions to be applied independently on the groups of a groupby object.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/gcalmettes/pandas-groupby-apply-chaining-extension",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
