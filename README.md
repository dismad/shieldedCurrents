
# Shielded Currents

A collection of Rust tools for analyzing Zcash shielded pool activity, transaction fees, supply history, and mempool data.

These tools were originally developed inside the [ZecHub wiki](https://github.com/ZecHub/zechub-wiki) and have been extracted into their own repository for easier use and maintenance.

## Tools

| Crate                        | Description                                                                 |
|-----------------------------|-----------------------------------------------------------------------------|
| `zcash-block-fees`          | Analyze block-level fee data                                                |
| `zcash-mempool-visualizer`  | Visualize / inspect current mempool state                                   |
| `zcash-shielded-currents`   | Track inflows/outflows and current activity across Transparent / Sapling / Orchard pools |
| `zcash-supply-history`      | Historical shielded supply tracking                                         |
| `zcash-tx-fee`              | Transaction fee analysis                                                    |

## Prerequisites

- Rust (stable)
- A running [Zebra](https://github.com/ZcashFoundation/zebra) node with RPC enabled
  (default: `http://127.0.0.1:8232`)

Most tools expect to talk to a local Zebra instance. Double check cookie location. Fork and customize as needed <3.

## Quick Start

```bash
git clone https://github.com/dismad/shieldedCurrents.git
cd shieldedCurrents
cargo build --release
```

### Build everything
```bash
cargo build --release
```

### Run a specific tool
```bash
cargo run -p zcash-shielded-currents --release
cargo run -p zcash-mempool-visualizer --release
cargo run -p zcash-block-fees --release
cargo run -p zcash-supply-history --release
cargo run -p zcash-tx-fee --release
```

Add `-- --help` (or the tool's own flags) after `--release` to see options for each binary.

## Workspace Structure

```text
shieldedCurrents/
├── Cargo.toml                  # workspace root
├── zcash-block-fees/
├── zcash-mempool-visualizer/
├── zcash-shielded-currents/
├── zcash-supply-history/
└── zcash-tx-fee/
```

This is a Cargo workspace. You can build or run any individual crate with `-p <name>` while still being able to build the entire set with a single command.

## Development

```bash
# Check everything
cargo check

# Format
cargo fmt

# Run tests (if any)
cargo test
```

## License

MIT OR Apache-2.0

---

