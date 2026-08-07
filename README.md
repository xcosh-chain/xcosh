Xcosh Core No Version

Copyright (c) 2026 Xcosh Core.
Copyright (c) 2026 AldianOkto (Subang, Indonesian). All rights reserved.
Distributed under the Apache License, Version 2.0, see the accompanying
file LICENSE or http://www.apache.org/licenses/LICENSE-2.0.
This product includes post-quantum cryptographic software utilizing
Cloudflare CIRCL (Dilithium Mode 3) and SHA-3 Keccak-256 hashing.


Introduction
-----
Xcosh Core is an experimental cryptocurrency built in Go, inspired by the early days of decentralized peer-to-peer digital cash. It utilizes Post-Quantum Cryptography featuring Dilithium Mode 3 signatures and SHA-3 Keccak-256 hashing algorithms combined with a Proof-of-Work (PoW) consensus mechanism.


Setup & Storage
-----
Xcosh automatically manages its operational data, ledger state, and wallet configuration securely in an external centralized directory at `~/.xcosh` to protect your assets even if the source repository folder is modified or removed.

Clone the repository and build the binary using Go:

  go build -o xcosh


To see all the feature functions, you can type the command below:

  ./xcosh --help

To get a wallet address or create one, you can type the following command:

  ./xcosh create [-label <account_label>]

To support the network by running a mining node, execute the manual mining command or keep the node running:

  ./xcosh mining <target_address>

Your computer will be solving computational problems using SHA-3 Keccak-256 and post-quantum Dilithium Mode 3 signatures that are used to lock in blocks of transactions. As a reward for supporting the network, you receive newly minted coins when you successfully generate a block.
