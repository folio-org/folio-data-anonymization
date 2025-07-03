# folio-data-anonymization

Copyright (C) 2025 The Open Library Foundation

This software is distributed under the terms of the Apache License,
Version 2.0. See the file "[LICENSE](LICENSE)" for more information.

These software library dependencies have non-permissive licenses:
* psycopg2: LGPL-3.0-or-later
* text-unidecode: Artistic-1.0-Perl OR GPL-1.0-only OR GPL-2.0-or-later

## Introduction
Folio Data Anonymization is a service that anonymizes or masks patron data in library records to ensure privacy protection.

## Dependency Management and Packaging
To install the dependencies, run:
- `pipx install poetry` or `pip install -r requirements.txt`
- `poetry install`

## Tests
Running the tests:
- `poetry run pytest tests`

## Airflow Setup and Security
Create a secret.yaml file:
```
apiVersion: v1
kind: Secret
metadata:
  name: airflow-user
type: Opaque
data:
  airflow-fernet-key: {any fernet key}
  airflow-password: {choose a password}
  airflow-secret-key: {any secret key}
  airflow-jwt-secret-key: {any JWT key}
```

To generate the secret key and the JWT key you may refer to https://docs.python.org/3/library/secrets.html for guidance.


Instructions on generating a Fernet key can be found at [How-to Guides: Securing Connections](https://airflow.apache.org/docs/apache-airflow/1.10.4/howto/secure-connections.html?highlight=fernet)
Example:
```
poetry run python3
>>> from cryptography.fernet import Fernet
>>> fernet_key= Fernet.generate_key()
>>> decoded_fernet_key = fernet_key.decode()
echo -n $decoded_fernet_key | base64
```

Once you have generated the desired keys and password apply it to your Kubernetes cluster using:
```
export NAMESPACE=<my-namespace>
kubectl -n $NAMESPACE apply -f secret.yaml`
```

## Install and maintain Apache Airflow in a Kubernetes cluster 
### Local development:
With [Docker Desktop](https://docs.docker.com/desktop/), [Helm](https://helm.sh/docs/intro/install/) and [kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl-macos/) installed, enable [Kubernetes in Docker Desktop](https://docs.docker.com/desktop/features/kubernetes/)

### Using Helm to deploy Airflow in the cluster:
```
export NAMESPACE=<my-namespace>
kubectl -n $NAMESPACE apply -f pv-volume.yaml
envsubst < airflow-values.yaml > ns-airflow-values.yaml
helm -n $NAMESPACE install --version 22.7.3 -f ns-airflow-values.yaml airflow oci://registry-1.docker.io/bitnamicharts/airflow
```

Note: in the `pv-volume.yaml` file you must use a storageClass that supports ReadWriteMany. If you do not specify a storageClassName, the default storageClass for your cluster will be used.


To upgrade or to reinitialize the airflow release when configuration changes are made, do:
```
envsubst < airflow-values.yaml > ns-airflow-values.yaml
export PASSWORD=$(kubectl get secret -n $NAMESPACE airflow-postgresql -o jsonpath="{.data.password}" | base64 -d)
helm -n $NAMESPACE upgrade --install --version 22.7.3 --set global.postgresql.auth.password=$PASSWORD -f ns-airflow-values.yaml airflow oci://registry-1.docker.io/bitnamicharts/airflow
```

## Run the DAGs to Anonymize Data
1. Either create an ingress for the airflow service that resolves to a hostname, or simply port-forward the airflow service to your local browser: `kubectl -n $NAMESPACE port-forward svc/airflow 8080:8080`
1. Open your browser and go to localhost:8080 (or the URL if you created a hostname and ingress).
1. Login with the user airflow and the airflow-password created with the secret.yaml file.
1. Create a new Connection under Admin > Connections.
1. Enter the database connection information. Use conn_id "postgres_folio".
1. Navigate back to the DAGs menu. Unpause the DAGs by toggling the on/off slider.
1. To anonymize data, trigger a dag run of the select_table_objects DAG. From the Trigger DAG menu, enter the batch size, select a configuration file, and enter the tenant ID to anonymize data for that schema. Once the select_table_object DAG run completes, it will trigger the anonymize_data DAG.
1. Once the anonymize_data DAG runs complete, check if any failed. If there are failures, the state can be cleared so that they are re-run. You can either re-run a whole DAG run by clearing its state or choose specific tasks within a DAG run to re-run.
1. Navigate back to the DAGs menu. Trigger the truncate_tables DAG. This will truncate the tables listed in folio_data_anonymization/plugins/truncate/truncate_schema_tables.json.

Refer to the [Apache Airflow Documentation](https://airflow.apache.org/docs/apache-airflow/2.10.5/ui.html) for navigating the UI.

## QA Anonymized Dataset with FOLIO UI
1. If you are re-using an existing FOLIO deployment, uninstall all the modules and okapi.
1. Update the database connection information for your FOLIO deployment to that of the anonymized database.
1. If you are re-using an existing FOLIO deployment, update the SYSTEM_USER_PASSWORD environment variable where it is used, if desired.
1. Install okapi and the modules for your FOLIO deployment.
1. Create a superuser for the anonymized dataset, whose credentials are to be shared.
1. Add the superuser to the acquisitions units that the dataset has.
1. Login to folio and check that functionality still works.

## JSON Configuration Files for Anonymizing Data
The JSON configuration files listed under folio_data_anonymization/plugins/config are used to determine how each jsonb field in a schema table is to be anonymized or set to empty. The JSON files contain a list of objects where each object is a database schema table to be processed. The keys for each object are table_name, anonymize, and set_to_empty. Under anonymize is the object jsonb, which contains a list of lists. Each outer list element is a list of two: the first inner list element is a jsonpath to the jsonb field data that is to be anonymized, and the second inner list element is the faker function to be used to anonymize the data. The project uses [jsonpath-ng](https://pypi.org/project/jsonpath-ng/) for JSONPath implementation and the [faker](https://pypi.org/project/Faker/) python package to anonymize data.

The Faker package allows one to extend the BaseProvider class in order to create faked data that adheres to some desired data type. For instance, in this project we've added a Users provider that fakes the user barcode, creating a string of 10 random digits. The providers we created are in folio_data_anonymization/plugins/providers.py.

Here is a short example of the JSON configuration file format:
```
[
  {
    "table_name": "mod_users.users",
    "anonymize": {
      "jsonb": [
        [
          "personal.addresses[*].addressLine1",
          "street_address"
        ],
        [
          "personal.addresses[*].addressLine2",
          "street_address"
        ]
      ]
    },
    "set_to_empty": {
      "jsonb": [
        "customFields"
      ]
    }
  }
]
```
