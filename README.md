# SDSS query analysis tools

A set of scripts that allow us to extract queries from logs, have sql server explain them and then analyze the results.

## Install

I suggest, you use a virtual environment.

Activate your virtual env and then install freetds and the dependencies.

```
brew install freetds
python setup.py develop
```

Create a copy of the config file and set the password.

```
cp default.ini default.ini.local
vim default.ini.local
```

## Run

You should be able to run `explain_queries default.ini.local`.
