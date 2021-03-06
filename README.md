# Data Loader

A Python script to load a specified amount of random documents into specified collections from the travel-sample dataset using the Couchbase Python SDK.

It creates the collections if they do not exist. It also creates the primary index.

Tested on Python 3.9.

## Running the Script

- Download the travel-sample dataset from the [github repo](https://github.com/couchbase/docloader/blob/master/examples/travel-sample.zip) and extract it to the local folder (script_folder/travel-sample).
- Install the requirements

  `$ pip install -r requirements.txt`

- Update the environment variables to match the Capella cluster to import the data into by creating a copy of [.env.example](.env.example) file and renaming it to `.env`

  > DB_HOST= couchbase-wan

  > DB_USER= db-user

  > DB_PASS= db-password

  > BUCKET= bucket-to-import

  > SCOPE= scope-to-import

  > DATA_DIR= path-to-travel-sample/docs

  > DB_DATA_NODE= srv-record-of-data-node

- Run the script

  `$ python import_data.py`

## Configuring the Data to Import

> NO_OF_SAMPLES = 100
> IMPORT_MATRIX = [
>
> > {"collection": "airline", "prefix": "inventory.airline"},
> > {"collection": "airport", "prefix": "inventory.airport"},
> > {"collection": "hotel", "prefix": "inventory.hotel"},
> > {"collection": "landmark", "prefix": "inventory.landmark"},
> > {"collection": "route", "prefix": "inventory.route"},
> > ]

NO_OF_SAMPLES: Documents to be sampled to import into collection

IMPORT_MATRIX: Key-Value pair of the collection along with the pattern to find the docs (filename-prefix)

- The collection refers to the collection in which to import the json documents matching the prefix in the DATA_DIR.
- The prefix for airline documents is inventory.airline.\*.json).
  Example: travel-sample/docs/inventory.airline.airline_10.json.
