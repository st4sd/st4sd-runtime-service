# Runtime service for the Simulation Toolkit for Scientific Discovery (ST4SD)

## Details

The `st4sd-runtime-service` container is deployed to OpenShift clusters using
the [ST4SD Deployment template](https://github.ibm.com/st4sd/st4sd-deployment). Access to the instantiated HTTPS service
is handled using [the OpenShift fork of the oauth-proxy side-car container](https://github.com/openshift/oauth-proxy).
Users which have access to the `Services` objects in the same namespace as the `st4sd-runtime-service` microservice are
authorized to access the microservice.

Runtime Service to create, manage, and monitor virtual experiments that execute using
the [st4sd-runtime-k8s](https://github.com/st4sd/st4sd-runtime-k8s) and
the [st4sd-runtime-core](https://github.com/st4sd/st4sd-runtime-core).

The OpenAPI specification of the runtime service is in [`docs/openapi.yaml`](docs/openapi.yaml).

## Quick links

- [Getting started](#getting-started)
- [Development](#development)
- [Help and Support](#help-and-support)
- [Contributing](#contributing)
- [License](#license)

## Getting started

### Requirements

#### Python

Running and developing this project requires a recent Python version, it is suggested to use Python 3.7 or above. You
can find instructions on how to install Python on the [official website](https://www.python.org/downloads/).

## Development

Coming soon.

### Installing dependencies

Install the dependencies for this project with:

```bash
pip install -r requirements.txt
```

### Developing locally

Coming soon.

### Lint and fix files

Coming soon.

## Help and Support

Please feel free to reach out to one of the maintainers listed in the [MAINTAINERS.md](MAINTAINERS.md) page.

## Contributing

We always welcome external contributions. Please see our [guidance](CONTRIBUTING.md) for details on how to do so.

## References

If you use ST4SD in your projects, please consider citing the following:

```bibtex
@software{st4sd_2022,
author = {Johnston, Michael A. and Vassiliadis, Vassilis and Pomponio, Alessandro and Pyzer-Knapp, Edward},
license = {Apache-2.0},
month = {12},
title = {{Simulation Toolkit for Scientific Discovery}},
url = {https://github.com/st4sd/st4sd-runtime-core},
year = {2022}
}
```

## License

This project is licensed under the Apache 2.0 license. Please [see details here](LICENSE.md).
