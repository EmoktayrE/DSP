Required Python packages
===

The packages required by the REST API are the following:
* flask
* flask_sqlalchemy
* sqlalchemy
* statsmodels
* pandas
* numpy

Database setup
===

As is, the API makes use of a local database. The database file is included in the repository. As set in `settings.py`, the database file is expected to be located in the same folder as the code.
It is also possible to start with a fresh database. In order to do so, remove or rename the existing database file and run the following in a Python REPL:
```python
from cmsamodels import db as db1
from gvbmodels import db as db2
db1.create_all()
db2.create_all()
```
This creates a new database file and the necessary tables in that database file, based on the datamodels defined in `cmsamodels.py` and `gvbmodels.py`.

Running the application
===

For testing, the application can be run by running the command `python prediction_api.py`. This will start a local development server, which is reachable at http://localhost:5000.
