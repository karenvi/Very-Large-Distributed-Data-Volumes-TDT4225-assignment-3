# Assignment 3

## MongoDB

### Virtual Machine

In order to connect to the VM, set `ENABLE_VM = 1` in the `.env` file.

### Local

Instructions for how to run a local instance of MongoDB.

#### Requirements

- Docker / Docker-Compose

#### Docker

To run via Docker:

```bash
docker run -p 27017:27017 --name mongodb mongo
```

#### Docker-Compose

To run via Docker-Compose:

```bash
docker-compose up --build
```

### Environment Variables

A `.env` file needs to be created in the root folder of the project.

See `.env.example` for the example config.
