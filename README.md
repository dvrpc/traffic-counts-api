# Traffic Counts API

API for DVRPC's traffic counts database, which is hosted on Oracle's cloud server and configured to allow connections from allow-listed IP addresses only. 

The following configuration variables should go in a git-ignored config.py file at the root level:
  * USER (database user)
  * PASSWORD (database password)
  * DB (database)
  * URL_ROOT (path preceding individual endpoints)

An example is provided at config.py.example.

See full documentation of the API at <https://cloud.dvrpc.org/api/traffic-counts/v1/docs> and <https://cloud.dvrpc.org/api/traffic-counts/v2/docs>.

## Development

A server on Digital Ocean (oracle-dev) has been added to the allow-list for development purposes. Use the requirements_dev.txt file to create a Python virtual environment, and then run `fastapi dev` from the activated virtual environment to start a development server. Use ssh tunneling to reach endpoints locally (e.g. `ssh -L 8000:127.0.0.1:8000 [host]` to access <http://localhost:8000/api/traffic-counts/v2/docs> in your browser).

## Production

Managed by [cloud-ansible project](https://github.com/dvrpc/cloud-ansible).
