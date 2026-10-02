# Shielded Currents

A collection of Rust tools for analyzing Zcash shielded pool activity, transaction fees, supply history, and mempool data.

These tools were originally developed inside the [ZecHub wiki](https://github.com/Zechub/zechub-wiki) and have been extracted into their own repository for easier use and maintenance.

Ironwood-ready (NU6.3) as of 2026-07-23. Tracks Transparent / Sapling / Orchard / Ironwood pools.

## Tools

| Crate | Description |
| --- | --- |
| `zcash-block-fees` | Block-level fee totals. Default path reads spent outputs from `getblock` verbosity 3. |
| `zcash-mempool-visualizer` | Visualize / inspect current mempool state. |
| `zcash-shielded-currents` | Inflows, outflows, and current activity across Transparent / Sapling / Orchard / Ironwood. Fee-enabled runs use `getblock` verbosity 3. |
| `zcash-supply-history` | Historical shielded supply tracking. |
| `zcash-tx-fee` | Transaction fee analysis. |

## Prerequisites

- Rust (stable)
- A running [Zebra](https://github.com/ZcashFoundation/zebra) node with RPC enabled (default: `http://127.0.0.1:8232`)

Node version depends on the tool:

- `zcash-block-fees` and fee-enabled `zcash-shielded-currents` require **zebrad >= 7.0.0-rc.0**. The default fee path is `getblock` verbosity 3 (`--fee-source prevout`). A 6.x node does not return `vin.prevout` and those runs will fail.
- `zcash-mempool-visualizer`, `zcash-supply-history`, and `zcash-tx-fee` still work with Zebra **>= 6.0.0**.
- `--fee-source legacy` keeps the old `getrawtransaction` input lookup. It runs on 6.x, and it is slower.

Most tools expect a local Zebra instance. Check the cookie location. Fork and customize as needed.

Ironwood (NU6.3) activates at height 3,428,143 (~2026-07-28). The node must be upgraded before that height.

## Fee path

`zcash-block-fees` and `zcash-shielded-currents` no longer fetch parent transactions to price inputs. Verbosity 3 returns `vin.prevout.value`. A missing prevout is excluded from the fee total and counted, not stored as zero.

`--skip-fees` on currents stays on verbosity 2. Supply history should stay on low verbosity. Neither benefits from verbosity 3.

On zebrad 7.0.0-rc.0, blocks 3503072–3504223 match at 296269451 zat with no missing prevouts. Block-fees dropped from 4.40s to 0.62s. Currents dropped from 2.36s to 0.56s. Over 8064 blocks, block-fees dropped from 33.09s to 3.81s with a zero fee delta.

## Quick Start

```bash
git clone https://github.com/dismad/shieldedCurrents.git
cd shieldedCurrents
cargo build --release
```

### Run a specific tool

```bash
cargo run -p zcash-shielded-currents --release -- --last 500
cargo run -p zcash-block-fees --release -- --last 500 --quiet
cargo run -p zcash-mempool-visualizer --release
cargo run -p zcash-supply-history --release
cargo run -p zcash-tx-fee --release
```

Add `-- --help` after `--release` for each binary's flags.

Compare the verbosity-3 path with the old input fetches on a fixed height range:

```bash
cargo run -p zcash-block-fees --release -- --compare --from 3503072 --to 3504223 --quiet
cargo run -p zcash-shielded-currents --release -- --compare --from 3503072 --to 3504223
```

Force the old path:

```bash
cargo run -p zcash-block-fees --release -- --fee-source legacy --last 500 --quiet
cargo run -p zcash-shielded-currents --release -- --fee-source legacy --last 500
```

## Workspace Structure

```text
shieldedCurrents/
├── Cargo.toml
├── zcash-block-fees/
├── zcash-mempool-visualizer/
├── zcash-shielded-currents/
├── zcash-supply-history/
└── zcash-tx-fee/
```

This is a Cargo workspace. Build or run one crate with `-p <name>`, or build the set with `cargo build --release`.

## Development

```bash
cargo check
cargo fmt
cargo test -p zcash-block-fees -p zcash-shielded-currents
```

The fee tests cover the status-quo formula and the verbosity-3 conversion. They do not need a node. `--compare` is the live check.

## License

MIT OR Apache-2.0
