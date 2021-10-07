import setuptools
 
with open("README.md", "r") as fh:
    long_description = fh.read()
 
setuptools.setup(
    name="relpipeline",
    version="1.0.0",
    author="Ken Tam",
    author_email="ken.tam@relativity.com",
    description="Package to test out packages",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=['relpipeline'],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
) 