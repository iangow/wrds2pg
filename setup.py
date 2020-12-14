import setuptools

with open("README.md", "r") as f:
    long_description = f.read()
    print(long_description)

setuptools.setup(
    name="wrds2pg",
    version="1.0.6",
    author="Ian Gow",
    author_email="iandgow@gmail.com",
    description="Import WRDS tables or SAS data into PostgreSQL.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/iangow/wrds2pg/",
    packages=setuptools.find_packages(),
    install_requires=['pandas', 'sqlalchemy', 'paramiko', 'psycopg2'],
    python_requires=">=3",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
