# Install

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

# Run

[wip]

You should be able to run `explain_queries default.ini.local`.
