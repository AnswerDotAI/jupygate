use jupygate::{cull_cutoff, should_cull};

#[test]
fn cull_idle_precedence_and_guards() {
    let t = 10_000.0;
    assert_eq!(cull_cutoff(t, 0.0, None, None), None);
    assert_eq!(cull_cutoff(t, 0.0, Some(0.0), None), None);
    assert_eq!(cull_cutoff(t, 0.0, Some(1800.0), None), Some(t - 1800.0));
    assert_eq!(cull_cutoff(t, 3600.0, Some(1800.0), None), Some(t - 1800.0));
    assert_eq!(cull_cutoff(t, 600.0, Some(1800.0), Some(1200.0)), Some(t - 600.0));
    let cutoff = cull_cutoff(t, 0.0, Some(1800.0), None).unwrap();
    assert!(should_cull(t - 1801.0, cutoff, false, false));
    assert!(!should_cull(t - 1799.0, cutoff, false, false));
    assert!(!should_cull(t - 5000.0, cutoff, true, false));
    assert!(!should_cull(t - 5000.0, cutoff, false, true));
}
