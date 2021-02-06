use crate::histogram::*;

const RANK_EPS_FOR_K_200: f64 = 0.0133;

fn delta(x: f64, y: f64) -> f64 {
    (x - y).abs()
}

#[test]
fn test_k_limits() {
    let _: Histogram<i64> = Histogram::new(MIN_K, DEFAULT_M);
    let _: Histogram<i64> = Histogram::new(MAX_K, DEFAULT_M);
}

#[test]
fn test_empty() {
    let sketch = Histogram::default();
    assert!(sketch.is_empty());
    assert!(!sketch.is_estimation_mode());
    assert!(sketch.count() == 0);
    assert_eq!(sketch.retained(), 0);
    assert!(sketch.rank(0).is_none());
}

#[test]
fn test_one_item() {
    let mut sketch = Histogram::default();
    sketch.insert(1);
    assert!(!sketch.is_empty());
    assert!(!sketch.is_estimation_mode());
    assert_eq!(sketch.count(), 1);
    assert_eq!(sketch.retained(), 1);
    assert_eq!(sketch.rank(1), Some(0.0));
    assert_eq!(sketch.rank(2), Some(1.0));
    assert_eq!(sketch.quantiles(&[0.0]).unwrap(), vec![&1]);
    assert_eq!(sketch.quantiles(&[0.5]).unwrap(), vec![&1]);
    assert_eq!(sketch.quantiles(&[1.0]).unwrap(), vec![&1]);
    let fractions = [0.0, 0.5, 1.0];
    let quantiles = sketch.quantiles(&fractions).unwrap();
    assert!(quantiles.len() == 3);
    assert!(*quantiles[0] == 1);
    assert!(*quantiles[1] == 1);
    assert!(*quantiles[2] == 1);
}

#[test]
fn test_10_items() {
    let mut sketch = Histogram::default();
    sketch.insert(1);
    sketch.insert(2);
    sketch.insert(3);
    sketch.insert(4);
    sketch.insert(5);
    sketch.insert(6);
    sketch.insert(7);
    sketch.insert(8);
    sketch.insert(9);
    sketch.insert(10);
    assert_eq!(sketch.quantiles(&[0.0]).unwrap(), vec![&1]);
    assert_eq!(sketch.quantiles(&[0.5]).unwrap(), vec![&6]);
    assert_eq!(sketch.quantiles(&[0.99]).unwrap(), vec![&10]);
    assert_eq!(sketch.quantiles(&[1.0]).unwrap(), vec![&10]);
}

#[test]
fn test_100_items() {
    let mut sketch = Histogram::default();
    for i in 0..100 {
        sketch.insert(i);
    }
    assert_eq!(sketch.quantiles(&[0.0]).unwrap(), vec![&0]);
    assert_eq!(sketch.quantiles(&[0.01]).unwrap(), vec![&1]);
    assert_eq!(sketch.quantiles(&[0.5]).unwrap(), vec![&50]);
    assert_eq!(sketch.quantiles(&[0.99]).unwrap(), vec![&99]);
    assert_eq!(sketch.quantiles(&[1.0]).unwrap(), vec![&99]);
}

#[test]
fn test_many_items_exact_mode() {
    let mut sketch = Histogram::default();
    const N: usize = 200;
    for i in 0..N {
        sketch.insert(i);
        assert!(sketch.count() == i + 1);
    }
    assert!(!sketch.is_empty());
    assert!(!sketch.is_estimation_mode());
    assert_eq!(sketch.retained(), N);

    let fractions = [0.0, 0.5, 1.0];
    let quantiles = sketch.quantiles(&fractions).unwrap();
    assert!(quantiles.len() == 3);
    assert!(*quantiles[0] == 0);
    assert!(*quantiles[1] == N / 2);
    assert!(*quantiles[2] == N - 1);

    for i in 0..N {
        let true_rank = i as f64 / N as f64;
        assert!(sketch.rank(i) == Some(true_rank));
    }
}
#[test]
fn test_many_items_estimation_mode() {
    let mut sketch = Histogram::default();
    let n = 1000000;
    for i in 0..n {
        sketch.insert(i);
        assert_eq!(sketch.count(), i + 1);
    }
    assert!(!sketch.is_empty());
    assert!(sketch.is_estimation_mode());

    // test rank
    for i in 0..n {
        let true_rank = i as f64 / n as f64;
        let estimate_rank = sketch.rank(i).unwrap();
        assert!(delta(estimate_rank, true_rank) < RANK_EPS_FOR_K_200);
    }

    // test quantiles at every 0.1 percentage point
    let fractions: Vec<_> = (0..=1000).map(|i| i as f64 / 1000.0).collect();
    let estimate_quantiles = sketch.quantiles(&fractions).unwrap();
    for i in 0..fractions.len() {
        let true_quantile = i * n / 1000;
        let estimate_quantile = *estimate_quantiles[i];
        let one_pct = n / 100;
        assert!(diff(estimate_quantile, true_quantile) < one_pct);
    }
}

#[test]
fn test_merge() {
    let mut sketch1 = Histogram::default();
    let mut sketch2 = Histogram::default();
    let n = 10000;
    for i in 0..n {
        sketch1.insert(i);
        sketch2.insert((2 * n) - i - 1);
    }

    let one_pct = n / 100;
    let quantiles1 = sketch1.quantiles(&[0.0, 1.0]).unwrap();
    assert!(diff(*quantiles1[0], 0) < one_pct);
    assert!(diff(*quantiles1[1], n - 1) < one_pct);
    let quantiles2 = sketch2.quantiles(&[0.0, 1.0]).unwrap();
    assert!(diff(*quantiles2[0], n) < one_pct);
    assert!(diff(*quantiles2[1], 2 * n - 1) < one_pct);

    let one_pct = n * 2 / 100;
    sketch1.merge(sketch2);
    assert!(!sketch1.is_empty());
    assert!(sketch1.count() == 2 * n);
    let quantiles1 = sketch1.quantiles(&[0.0, 0.5, 1.0]).unwrap();
    assert!(diff(*quantiles1[0], 0) < one_pct);
    assert!(diff(*quantiles1[1], n - 1) < one_pct);
    assert!(diff(*quantiles1[2], 2 * n - 1) < one_pct);
}

fn diff(x: usize, y: usize) -> usize {
    if x > y {
        x - y
    } else {
        y - x
    }
}
