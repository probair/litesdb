<!--
Copyright (c) 2026 LiteSDB contributors.
SPDX-License-Identifier: LGPL-3.0-only
This file is part of LiteSDB. See LICENSE for license details.
Project: https://github.com/probair/litesdb
-->

# LiteSDB

LiteSDB is a lightweight embedded time-series database tailored for server monitoring.

Multiple logical databases can share one buffered physical WAL while keeping independent tails, manifests and sealed files.
See the shared storage APIs in `litesdb-core`; old storage formats are explicitly rejected.

## Advantages

The following figures are historical single-run engineering measurements from the earlier standalone-WAL
environment. They are for reference only and are not guarantees.

- Minimal storage cost: about **5.0 MB per host for 30 days** under the reference workload.
- Minimal memory footprint: about **512 KiB** incremental RSS for an empty database.
- Small binary: about **689 KB** for the release CLI.

## Modules

- [`core/`](core/): embedded storage engine.
- [`cli/`](cli/): command-line adapter.
