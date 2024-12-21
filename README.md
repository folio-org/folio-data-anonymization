## folio-data-anonymization
Folio Data Anonymization is a service that anonymizes or masks patron data in library records to ensure privacy protection.

## Environmental Variables 
After cloning this repository, create a local .env file and add the following variables:

```bash
export DB_HOST=example.com
export DB_PORT=5432
export DB_USER=username
export DB_PASS=password
export DB_NAME=dbname
export TENANT=tenant
```

## Dependency Management and Packaging
To install the dependencies, run:
- `pipx install poetry`
- `poetry install`

## Running
- `source .env`
- `poetry run`

## Tests
- `poetry run pytest tests`