# Library to convert WRDS SAS data

This package was created to convert [WRDS](https://wrds-web.wharton.upenn.edu/wrds/) SAS data to modern data formats.
This package has three major functions, one for each of three popular data formats.

 - `wrds_update()`: Imports WRDS SAS data into a PostgreSQL database.
 - `wrds_update_pq()`: Converts WRDS SAS data to parquet files.
 - `wrds_update_csv()`: Converts WRDS SAS data to gzipped CSV files.

This package was primarily designed to handle WRDS data, but some support is provided for importing a local SAS file (`*.sas7dbat`) into a PostgreSQL database.
Functions prefixed with `wrds_` are designed to pull data from WRDS via SSH.
For local SAS datasets, use functions that accept `fpath` (e.g., `sas_to_pandas()` and related helpers).

## Requirements

### 1. Python

The software uses Python 3 and depends on SQLAlchemy and Paramiko.
Some helper functions return Pandas DataFrames; Pandas is optional unless those functions are used.

### 2. A WRDS ID

To access WRDS non-interactively (e.g., from Python scripts), you must use
**SSH public-key authentication**.

WRDS provides a dedicated SSH endpoint for key-based authentication:

`wrds-cloud-sshkey.wharton.upenn.edu`

#### Step 1: Generate a modern SSH key (recommended)
WRDS supports modern SSH key types. We recommend **ed25519**:

`ssh-keygen -t ed25519 -C "your_wrds_id@wrds"`

Accept the default location (`~/.ssh/id_ed25519`).

You may use a passphrase if your SSH agent is running.
For unattended jobs (cron / CI), an empty passphrase may be required.

#### Step 2: Install the public key on WRDS
Copy your public key to the WRDS SSH-key host:

```
cat ~/.ssh/id_ed25519.pub | \
ssh your_wrds_id@wrds-cloud-sshkey.wharton.upenn.edu \
  "mkdir -p ~/.ssh && chmod 700 ~/.ssh && \
   cat >> ~/.ssh/authorized_keys && chmod 600 ~/.ssh/authorized_keys"
```

If `~/.ssh` does not exist on WRDS, the command above will create it.

#### Step 3: (Recommended) Configure SSH
Add an entry to `~/.ssh/config`:

```
Host wrds
    HostName wrds-cloud-sshkey.wharton.upenn.edu
    User your_wrds_id
    IdentityFile ~/.ssh/id_ed25519
    IdentitiesOnly yes
```
You can now connect with:

```
ssh wrds
```

This configuration is also used automatically by `paramiko`, enabling
password-less access from Python.

#### Troubleshooting
If SSH still prompts for a password, run:

```
ssh -vvv wrds
```

and confirm that `publickey` appears in the list of authentication methods.

`wrds2pg` uses `paramiko` to execute SAS code on WRDS via SSH.
Password-based authentication will not work in unattended scripts.

### 3. PostgreSQL

For the `wrds_update()` function, you should have write access to a PostgreSQL database to store the data.

### 4. Environment variables

Environment variables that the code can use include:

- `PGDATABASE`: The name of the PostgreSQL database you use.
- `PGUSER`: Your username on the PostgreSQL database.
- `PGHOST`: Where the PostgreSQL database is to be found (this will be `localhost` if it's on the same machine as you're running the code on)
- `WRDS_ID`: Your [WRDS](https://wrds-web.wharton.upenn.edu/wrds/) ID.
- `DATA_DIR`: The local repository for parquet files.
- `CSV_DIR`: The local repository for compressed CSV files.

You can set these environment variables in (say) `~/.zprofile`:

```bash
export PGHOST="localhost"
export PGDATABASE="crsp"
export WRDS_ID="iangow"
export PGUSER="igow"
```

## Using `wrds_update()`.

Two arguments `table_name` and `schema` are required.

### 1. WRDS Settings
Set `WRDS_ID` using either `wrds_id=your_wrds_id` in the function call or the environment variable `WRDS_ID`.

### 2. Environment variables

The `wrds_update()` function will use the environment variables `PGHOST`, `PGDATABASE`, and `PGUSER` if you have set them. 
Otherwise, you need to provide values as arguments to `wrds_udpate()`. 
The default for `PGPORT` is `5432`.

### 3. Table settings
To tailor your request, specify the following arguments:

- `fix_missing`: set to `True` to fix missing values. 
This addresses special missing values, which SAS's `PROC EXPORT` dumps as strings.
The default is `False`. 
- `fix_cr`: set to `True` to fix characters. Default value is `False`.
- `drop`: specify columns to be dropped using SAS syntax (e.g., `drop="id name"` will drop columns `id` and `name`).
- `obs`: specify the maximum number of observations to download (e.g., `obs=10` will import the first 10 rows from the table on WRDS).
- `rename`: rename columns (e.g., `rename="fee=mngt_fee"` renames `fee` to `mngt_fee`).
- `force`: set to `True` to force update. Default value is `False`.

## Importing local SAS data into PostgreSQL

The software can also upload a local SAS file to PostgreSQL. 
You need to have local SAS in order to use this function.
Use `fpath` to specify the path to the file to be imported.

### Examples

This software is available from [PyPI](https://pypi.org/project/wrds2pg/).
To install of `wrds2pg` from there:

```bash
pip3 install wrds2pg
```

To install the development version `wrds2pg` from Github:

```bash
sudo -H pip3 install git+https://github.com/iangow/wrds2pg --upgrade
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

# 2. Upload test.sas7bdat to pg as crsp.mcti
wrds_update(table_name="mcti", schema="crsp", fpath="your_path/test.sas7bdat")
```

### Report bugs

Author: Ian Gow, <iandgow@gmail.com>
Contributors: Jingyu Zhang, <jingyu.zhang@chicagobooth.edu>, Evan Jo.
