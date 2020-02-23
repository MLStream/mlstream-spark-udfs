import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="mlstream-spark-udfs",
    version="0.0.1",
    author="MLStream",
    author_email="mlstream@mlstream.com",
    description="User-defined types for pyspark SQL",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/MLStream/mlstream-spark-udfs",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache",
        "Operating System :: Linux",
    ],
    python_requires='>=2.7',
)
