## WRDS to PG Migration
This software has two functions:
- Download tables from [WRDS](https://wrds-web.wharton.upenn.edu/wrds/) and uploads to PG. 
- Upload sas file (`*.sas7dbat`) to PG.

The code will only work if you have access to WRDS and to the data in question.
## Requirements

### 2. Python
The software uses Python 3 and depends on Pandas, SQLAlchemy and Paramiko. In addition, the Python scripts generally interact with PostgreSQL using SQLAlchemy and the [Psycopg](http://initd.org/psycopg/) library.

### 3. A WRDS ID
To use public-key authentication to access WRDS, follow hints taken from [here](https://debian-administration.org/article/152/Password-less_logins_with_OpenSSH) to set up a public key.
Copy that key to the WRDS server from the terminal on my computer. 
(Note that this code assumes you have a directory `.ssh` in your home directory. If not, log into WRDS via SSH, then type `mkdir ~/.ssh` to create this.) 
Here's code to create the key and send it to WRDS (for me):
```
ssh-keygen -t rsa
cat ~/.ssh/id_rsa.pub | ssh iangow@wrds-cloud.wharton.upenn.edu "cat >> ~/.ssh/authorized_keys"
```
Use an empty passphrase in setting up the key so that the scripts can run without user intervention.

### 4. PostgreSQL
You should have a PostgreSQL database to store the data. There are also some data dependencies in that some scripts assume the existence of other data in the database. Also, I assume the existence of a role `wrds` (SQL `CREATE ROLE wrds` works to add this if it is absent).

### 5. Environment variables

Environment variables that the code can use include:

- `PGDATABASE`: The name of the PostgreSQL database you use.
- `PGUSER`: Your username on the PostgreSQL database.
- `PGHOST`: Where the PostgreSQL database is to be found (this will be `localhost` if its on the same machine as you're running the code on)
- `WRDS_ID`: Your [WRDS](https://wrds-web.wharton.upenn.edu/wrds/) ID.

I set these environment variables in `~/.profile`:

```
export PGHOST="localhost"
export PGDATABASE="crsp"
export WRDS_ID="iangow"
export PGUSER="igow"
```

## Using the function `wrds_update`.

### 1. WRDS Settings
Set `WRDS_ID`  using either `wrds_id=your_wrds_id` in the function call or the environment variable `WRDS_ID`.

### 2. PG Settings
The software will use the environment variables `PGHOST`, `PGDATABASE`, and `PGUSER` if you If you have set them. Otherwise, you need to provide values as arguments to `wrds_udpate()`. Default `PGPORT` is`5432`.

Two arguments `table_name` and `schema` are required.

### 3. Table Settings
To tailor tables, specify the following variables:

- `fix_missing`: set to `True` to fix missing values. Default value is `False`. 
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

```
sudo -H pip3 install git+https://github.com/iangow/wrds2pg --upgrade
```

This software is also available from PyPI. To install it from [PyPI](https://pypi.org/project/wrds2pg/):
```
pip3 install wrds2pg
```
Examples of using this library:
```sh
source ~/.profile
```
```py
from wrds2pg import wrds2pg

# 1. Download crsp.mcti from wrds and upload to pg as crps.mcti
# Simplest version
wrds2pg.wrds_update(table_name="mcti", schema="crsp")
# Tailor table to your needs
wrds2pg.wrds_update(table_name="mcti", schema="crsp", host=your_pghost, dbname=your_pg_database, fix_missing=True, 
	fix_cr=True, drop="b30ret b30ind", obs=10, rename="caldt=calendar_date", force=True)

# 2. Upload test.sas7dbat to pg as crsp.mcti
wrds2pg.wrds_update(table_name="mcti", schema="crsp", fpath="your_path/test.sas7dbat")
```


### Report Bugs
Author: Ian Gow, <ian.gow@unimelb.edu.au>

Contributor: Jingyu Zhang, <jingyu.zhang@chicagobooth.edu>
