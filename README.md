# BetEdge Data

## High Level

This is a data platform currently utilizing ThetaData as a data vendor that allows for easy data gathering for historical stock and option data.
The platform exposes an API running at localhost:8000 which accepts POST requests to retrieve data from the underlying ThetaData API and store it locally in MinIO object store. You can access the MinIO interface at localhost:9091. The default username and password are minioadmin and minioadmin123 respectively. You will obviously need to be running these services to access these endpoints which you can do with a simple `docker compose up` or `docker compose up --build` command.

In order to access the ThetaData API you will to supply a `.env` file in the project root with valid values for `THETA_USERNAME` and `THETA_PASSWORD`. The rest of the config is in the modules with the code it relates to. The defaults provide sensible values but can be tweaked if desired.