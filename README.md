## WRDS to PG Migration
This software has two functions:
- Download tables from [WRDS](https://wrds-web.wharton.upenn.edu/wrds/) and uploads to PG. 
- Upload sas file (`*.sas7dbat`) to PG.

The code will only work if you have access to WRDS and to the data in question.
## Requirements
#### 1. Git
While not strictly necessary to use the scripts here, [Git](https://git-scm.com/downloads) likely makes it easier to download and to update.

If all Git repositories are kept in `~/git`, use the following commands to clone this repository:
```
cd ~/git
git clone https://github.com/iangow/wrds2pg.git
```
This will create a copy of the repository in `~/git/wrds2pg`. Note that one can get updates to the repository by going to the directory and "pulling" the latest code:
```
cd ~/git/wrds2pg
git pull
```
Alternatively, you can fork the repository on GitHub and then clone. Cloning using the SSH URL (e.g., `git@github.com:iangow/wrds2pg.git`) is necessary for Git pulling and pushing to work well in RStudio.

#### 2. Python
The software uses Python 3 and depends on Pandas, SQLAlchemy and Paramiko. In addition, the Python scripts generally interact with PostgreSQL using the psycopg (see [here](http://initd.org/psycopg/)) and SQLAlchemy.

#### 3. A WRDS ID
To use public-key authentication to access WRDS, follow hints taken from [here](https://debian-administration.org/article/152/Password-less_logins_with_OpenSSH) to set up a public key. Copy that key to the WRDS server from the terminal on my computer. (Note that this code assumes you have a directory `.ssh` in your home directory. If not, log into WRDS via SSH, then type `mkdir ~/.ssh` to create this.) Here's code to create the key and send it to WRDS (for me):
```
ssh-keygen -t rsa
cat ~/.ssh/id_rsa.pub | ssh iangow@wrds-cloud.wharton.upenn.edu "cat >> ~/.ssh/authorized_keys"
```
Use an empty passphrase in setting up the key so that the scripts can run without user intervention.

#### 4. PostgreSQL
You should have a PostgreSQL database to store the data. There are also some data dependencies in that some scripts assume the existence of other data in the database. Also, I assume the existence of a role `wrds` (SQL `CREATE ROLE wrds` works to add this if it is absent).

#### 5. Environment variables
I am migrating the scripts, etc., from using hard-coded values (e.g., my WRDS ID `iangow`) to using environment variales. 
Environment variables that I use include:

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

```
source ~/.profile
```

I also set them in `~/.Rprofile`, as RStudio doesn't seem to pick up the settings in `~/.profile` in recent versions of OS X:

```
Sys.setenv(PGHOST="localhost")
Sys.setenv(PGDATABASE="crsp")
```

## Settings
#### 1. WRDS Settings
Set `WRDS_ID` with `wrds_id=your_wrds_id`, otherwise the software will grep from OS environment variables. If you follow the instructions above closely, you don't need to do anything.

#### 2. PG Settings
If you have set `PGHOST`, `PGDATABASE`, `PGUSER` as environment variables, the software can grep them. Otherwise, users are expected to specify them when using `wrds_udpate()`. Default `PGPORT` is`5432`. Again, if you follow the instructions above closely, you don't need to do anything.

Two variables `table` and `schema` are required.

#### 3. Table Settings
To tailor tables, specify the following variables:

`fix_missing`: set to `True` to fix missing values. Default value is `False`. 

`fix_cr`: set to `True` to fix characters. Default value is `False`.

`drop`: add column names to be dropped.eg.`drop="id name"` will drop column `id` and `name`.

`obs`: add maxium number of observations. eg.`obs=10` will export the top 10 rows from the table.

`rename`: rename columns. eg.`rename="fee=mngt_fee"` rename `fee` to `mngt_fee`.

`force`: set to `True` to force update. Default value is `False`.

### Upload SAS File
The software can also upload SAS file directly to PG. You need to have local SAS in order to use this function.

Use `fpath` to specify file path.

### Examples
Here are some examples.

```py
from wrds2pg import wrds_update

# 1. Download crsp.mcti from wrds and upload to pg as crps.mcti
# Simplest version
wrds_update(table="mcti", schema="crsp")
# Tailor table to your needs
wrds_update(table="mcti", schema="crsp", host=your_pghost, dbname=your_pg_database, fix_missing=True, 
	fix_cr=True, drop="b30ret b30ind", obs=10, rename="caldt=calendar_date", force=True)

# 2. Upload test.sas7dbat to pg as crsp.mcti
wrds_update(table="mcti", schema="crsp", fpath="your_path/test.sas7dbat")
```

### Report Bugs
Author: Ian Gow, <ian.gow@unimelb.edu.au>

Contributor: Jingyu Zhang, <jingyu.zhang@chicagobooth.edu>