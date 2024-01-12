import setuptools

with open("README.md", "r") as f:
    long_description = f.read()
    print(long_description)

setuptools.setup(
    name="wrds2pg",
    version="1.0.31",
    author="Ian Gow",
    author_email="iandgow@gmail.com",
    description="Convert WRDS or local SAS data to PostgreSQL, parquet, or CSV.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/iangow/wrds2pg/",
    packages=setuptools.find_packages(),
    install_requires=['pandas', 'sqlalchemy>=2.0.0', 'paramiko', 'psycopg[binary]', 'pyarrow', 'duckdb'],
    python_requires=">=3",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
