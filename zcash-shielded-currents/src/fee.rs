use serde_json::Value;

pub fn consensus_fee_zats(
    vin_sum: i64,
    vout_sum: i64,
    vpub_old: i64,
    vpub_new: i64,
    sapling: i64,
    orchard: i64,
    ironwood: i64,
) -> i64 {
    vin_sum - vout_sum - vpub_old + vpub_new + sapling + orchard + ironwood
}

pub fn zec_to_zat(value: f64) -> i64 {
    (value * 100_000_000.0).round() as i64
}

pub fn is_coinbase(tx: &Value) -> bool {
    tx["vin"]
        .as_array()
        .and_then(|v| v.first())
        .and_then(|v| v["coinbase"].as_str())
        .is_some()
}

pub fn pool_parts(tx: &Value) -> (i64, i64, i64, i64, i64, i64) {
    let vout_sum = tx["vout"]
        .as_array()
        .map(|a| a.iter().filter_map(|v| v["valueZat"].as_i64()).sum())
        .unwrap_or(0);
    let mut vpub_old = 0i64;
    let mut vpub_new = 0i64;
    if let Some(js) = tx["vjoinsplit"].as_array() {
        for j in js {
            vpub_old += j["vpub_oldZat"].as_i64().unwrap_or(0);
            vpub_new += j["vpub_newZat"].as_i64().unwrap_or(0);
        }
    }
    let sapling = tx["valueBalanceZat"].as_i64().unwrap_or(0);
    let orchard = tx["orchard"]
        .as_object()
        .and_then(|o| o["valueBalanceZat"].as_i64())
        .unwrap_or(0);
    let ironwood = tx["ironwood"]
        .as_object()
        .and_then(|o| o["valueBalanceZat"].as_i64())
        .unwrap_or(0);
    (vout_sum, vpub_old, vpub_new, sapling, orchard, ironwood)
}

/// Status quo. A missing parent value contributes 0 and increments the miss count.
pub fn legacy_fee_zats(tx: &Value, mut lookup: impl FnMut(&str, u64) -> Option<i64>) -> (i64, usize) {
    if is_coinbase(tx) {
        return (0, 0);
    }
    let mut vin_sum = 0i64;
    let mut misses = 0usize;
    if let Some(vins) = tx["vin"].as_array() {
        for vin in vins {
            if let (Some(txid), Some(idx)) = (vin["txid"].as_str(), vin["vout"].as_u64()) {
                match lookup(txid, idx) {
                    Some(v) => vin_sum += v,
                    None => misses += 1,
                }
            }
        }
    }
    let (vout_sum, vpub_old, vpub_new, sapling, orchard, ironwood) = pool_parts(tx);
    (
        consensus_fee_zats(vin_sum, vout_sum, vpub_old, vpub_new, sapling, orchard, ironwood),
        misses,
    )
}

/// Verbosity 3. None if any transparent input lacks prevout.value.
/// A missing prevout is not treated as zero.
pub fn prevout_fee_zats(tx: &Value) -> Option<i64> {
    if is_coinbase(tx) {
        return Some(0);
    }
    let mut vin_sum = 0i64;
    if let Some(vins) = tx["vin"].as_array() {
        for vin in vins {
            if vin["txid"].as_str().is_none() {
                continue;
            }
            let value = vin
                .get("prevout")
                .and_then(|p| p.get("value"))
                .and_then(|v| v.as_f64())?;
            vin_sum += zec_to_zat(value);
        }
    }
    let (vout_sum, vpub_old, vpub_new, sapling, orchard, ironwood) = pool_parts(tx);
    Some(consensus_fee_zats(
        vin_sum, vout_sum, vpub_old, vpub_new, sapling, orchard, ironwood,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn transparent() -> Value {
        json!({
            "txid": "aa",
            "vin": [{"txid": "parent", "vout": 1}],
            "vout": [{"valueZat": 50_000_000}],
            "valueBalanceZat": 0
        })
    }

    #[test]
    fn status_quo_formula_matches_known_pools() {
        assert_eq!(consensus_fee_zats(100, 40, 0, 0, 0, 0, 0), 60);
        assert_eq!(consensus_fee_zats(100, 40, 30, 5, 0, 0, 0), 35);
        assert_eq!(consensus_fee_zats(0, 0, 0, 0, 10_000, 0, 0), 10_000);
        assert_eq!(consensus_fee_zats(0, 20, 0, 0, 0, 25, 0), 5);
        assert_eq!(consensus_fee_zats(0, 0, 0, 0, 1, 2, 3), 6);
    }

    #[test]
    fn zec_float_round_trips_zatoshis() {
        assert_eq!(zec_to_zat(1.23456789), 123_456_789);
        assert_eq!(zec_to_zat(0.00000001), 1);
        assert_eq!(zec_to_zat(0.0), 0);
    }

    #[test]
    fn legacy_uses_lookup_and_counts_misses_as_zero() {
        let tx = transparent();
        let (fee, misses) = legacy_fee_zats(&tx, |txid, vout| {
            assert_eq!(txid, "parent");
            assert_eq!(vout, 1);
            Some(50_000_100)
        });
        assert_eq!(fee, 100);
        assert_eq!(misses, 0);

        let (fee, misses) = legacy_fee_zats(&tx, |_, _| None);
        assert_eq!(fee, -50_000_000);
        assert_eq!(misses, 1);
    }

    #[test]
    fn prevout_matches_legacy_and_refuses_partial_inputs() {
        let mut tx = transparent();
        tx["vin"][0]["prevout"] = json!({"generated": false, "height": 10, "value": 0.500001});
        assert_eq!(prevout_fee_zats(&tx), Some(100));

        let (legacy, misses) = legacy_fee_zats(&tx, |_, _| Some(50_000_100));
        assert_eq!(misses, 0);
        assert_eq!(prevout_fee_zats(&tx), Some(legacy));

        tx["vin"][0].as_object_mut().unwrap().remove("prevout");
        assert_eq!(prevout_fee_zats(&tx), None);
    }

    #[test]
    fn shielded_and_sprout_agree_across_paths() {
        let tx = json!({
            "vin": [{"txid": "p", "vout": 0, "prevout": {"value": 1.0}}],
            "vout": [{"valueZat": 20_000_000}],
            "vjoinsplit": [{"vpub_oldZat": 5_000, "vpub_newZat": 1_000}],
            "valueBalanceZat": 2_000,
            "orchard": {"valueBalanceZat": 3_000, "actions": [{}]},
            "ironwood": {"valueBalanceZat": 4_000, "actions": [{}]}
        });
        let expected = consensus_fee_zats(100_000_000, 20_000_000, 5_000, 1_000, 2_000, 3_000, 4_000);
        assert_eq!(prevout_fee_zats(&tx), Some(expected));
        assert_eq!(legacy_fee_zats(&tx, |_, _| Some(100_000_000)).0, expected);
    }

    #[test]
    fn coinbase_fee_is_zero_on_both_paths() {
        let tx = json!({"vin": [{"coinbase": "00"}], "vout": [{"valueZat": 250_000_000}]});
        assert_eq!(legacy_fee_zats(&tx, |_, _| Some(1)).0, 0);
        assert_eq!(prevout_fee_zats(&tx), Some(0));
    }
}
