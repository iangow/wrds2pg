# Library to convert WRDS SAS data

This package was created to convert [WRDS](https://wrds-web.wharton.upenn.edu/wrds/) SAS data to modern data formats.
This package has three major functions, one for each of three popular data formats.

 - `wrds_update()`: Imports WRDS SAS data into a PostgreSQL database.
 - `wrds_update_pq()`: Converts WRDS SAS data to parquet files.
 - `wrds_update_csv()`: Converts WRDS SAS data to gzipped CSV files.

This package was primarily designed to handle WRDS data, but some support is provided for importing a local SAS file (`*.sas7dbat`) into a PostgreSQL database.

## Requirements

### 1. Python
The software uses Python 3 and depends on Pandas, SQLAlchemy and Paramiko. In addition, the Python scripts generally interact with PostgreSQL using SQLAlchemy and the [Psycopg](http://initd.org/psycopg/) library. 
These dependencies are installed when you use Pip (see instructions below).

### 2. A WRDS ID
To use public-key authentication to access WRDS, follow hints taken from [here](https://debian-administration.org/article/152/Password-less_logins_with_OpenSSH) to set up a public key.
Copy that key to the WRDS server from the terminal on your computer. 
(Note that this code assumes you have a directory `.ssh` in your home directory. If not, log into WRDS via SSH, then type `mkdir ~/.ssh` to create this.) 
Here's code (for me) to create the key and send it to WRDS:

```bash
ssh-keygen -t rsa
cat ~/.ssh/id_rsa.pub | ssh iangow@wrds-cloud.wharton.upenn.edu "cat >> ~/.ssh/authorized_keys"
```

Use an empty passphrase in setting up the key so that the scripts can run without user intervention.

### 3. PostgreSQL
For the `wrds_update()` function, you should have write access to a PostgreSQL database to store the data.

### 4. Environment variables

Environment variables that the code can use include:

- `PGDATABASE`: The name of the PostgreSQL database you use.
- `PGUSER`: Your username on the PostgreSQL database.
- `PGHOST`: Where the PostgreSQL database is to be found (this will be `localhost` if its on the same machine as you're running the code on)
- `WRDS_ID`: Your [WRDS](https://wrds-web.wharton.upenn.edu/wrds/) ID.
- `DATA_DIR`: The local repository for parquet files.
- `CSV_DIR`: The local repository for compressed CSV files.

Once can set these environment variables in (say) `~/.zprofile`:

```bash
export PGHOST="localhost"
export PGDATABASE="crsp"
export WRDS_ID="iangow"
export PGUSER="igow"
```

## Using the function `wrds_update`.

Two arguments `table_name` and `schema` are required.

### 1. WRDS Settings
Set `WRDS_ID`  using either `wrds_id=your_wrds_id` in the function call or the environment variable `WRDS_ID`.

### 2. Environment variables
The `wrds_udpate()` function will use the environment variables `PGHOST`, `PGDATABASE`, and `PGUSER` if you If you have set them. Otherwise, you need to provide values as arguments to `wrds_udpate()`. Default `PGPORT` is`5432`.

### 3. Table settings
To tailor your request, specify the following arguments:

- `fix_missing`: set to `True` to fix missing values. This addresses special missign values, which SAS's `PROC EXPORT` dumps as strings. Default value is `False`. 
- `fix_cr`: set to `True` to fix characters. Default value is `False`.
- `drop`: add column names to be dropped (e.g., `drop="id name"` will drop columns `id` and `name`).
- `obs`: add maxium number of observations (e.g., `obs=10` will import the first 10 rows from the table on WRDS).
- `rename`: rename columns (e.g., `rename="fee=mngt_fee"` renames `fee` to `mngt_fee`).
- `force`: set to `True` to force update. Default value is `False`.

## Importing SAS data into PostgreSQL
The software can also upload SAS file directly to PostgreSQL. 
You need to have local SAS in order to use this function.
Use `fpath` to specify the path to the file to be imported

### Examples
Here are some examples.

If you are at the home directory of this git repo, you can import and use the software as shown below.

To install it from Github:

```bash
sudo -H pip3 install git+https://github.com/iangow/wrds2pg --upgrade
```

This software is also available from PyPI. To install it from [PyPI](https://pypi.org/project/wrds2pg/):

```bash
pip3 install wrds2pg
```
Example usage:
```python
from wrds2pg import wrds_update

# 1. Download crsp.mcti from wrds and upload to pg as crps.mcti
# Simplest version
wrds_update(table_name="mcti", schema="crsp")

# Tailored arguments 
wrds_update(table_name="mcti", schema="crsp", host=your_pghost, 
	dbname=your_pg_database, 
	fix_missing=True, fix_cr=True, drop="b30ret b30ind", obs=10, 
	rename="caldt=calendar_date", force=True)

# 2. Upload test.sas7dbat to pg as crsp.mcti
wrds_update(table_name="mcti", schema="crsp", fpath="your_path/test.sas7dbat")
```

### Report bugs
Author: Ian Gow, <iandgow@gmail.com>
Contributors: Jingyu Zhang, <jingyu.zhang@chicagobooth.edu>, Evan Jo.
