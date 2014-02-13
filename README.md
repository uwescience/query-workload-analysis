# Query workload analysis tools

A set of scripts that allow us to extract queries from logs, have sql server explain them and then analyze the results.

Built for the SDSS sky survey and SQLShare logs.

## Install

I suggest, you use a virtual environment.

Activate your virtual env and then install freetds and the dependencies.

### First, install freetds

Mac os

```bash
brew install freetds freetype
```

Ubuntu

```bash
sudo apt-get install freetds-dev
```

### Then install the tools

```bash
pip install -r requirements.txt
pip install -e .  # or `python setup.py develop` if you are a developer
```

Create a copy of the config file and set the password.

```bash
cp default.ini default.ini.local
vim default.ini.local
```

## Run

You should be able to run `qwla --help`.

## Trubleshooting

If you get an error on a mac like `/usr/local/include/ft2build.h:56:10: fatal error: ‘freetype/config/ftheader.h’ file not found`, create a symlink to freetype: `ln -s /usr/local/opt/freetype/include/freetype2 /usr/local/include/freetype`.

## Uses

* [dataset](http://dataset.readthedocs.org/en/latest/) to work efficiently with data
* [SqlAlchemy](http://www.sqlalchemy.org/) to abstract from the database
* [docopt](http://docopt.org/) for the command line interface
* [pymssql](https://github.com/pymssql/pymssql) to connect to ms sql server
