use foundationdb::*;
#[allow(unused_imports)]
use foundationdb_macros::cfg_api_versions;
use foundationdb_sys::if_cfg_api_versions;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

mod common;

#[test]
fn test_runner_hooks() {
    let _guard = unsafe { foundationdb::boot() };
    futures::executor::block_on(test_happy_path_instrumented()).expect("failed to run");
    futures::executor::block_on(test_versionstamp_direct_api()).expect("failed to run");
    futures::executor::block_on(test_versionstamp_not_resolved_when_not_wanted())
        .expect("failed to run");
    futures::executor::block_on(test_versionstamp_resolved_when_wanted()).expect("failed to run");
    // ReportConflictingKeys (option 712) requires FDB >= 6.3
    if_cfg_api_versions!(min = 630 => {
        futures::executor::block_on(test_conflict_reports_in_metrics()).expect("failed to run");
        futures::executor::block_on(test_conflict_keys_direct_api()).expect("failed to run");
    });
}

/// Happy path: instrumented_run completes with metrics, no conflicts.
async fn test_happy_path_instrumented() -> FdbResult<()> {
    let db = common::database().await?;

    let (result, metrics) = db
        .instrumented_run(|trx, _| async move {
            trx.set(b"test_runner_hooks_happy", b"value");
            Ok(42u64)
        })
        .await
        .expect("transaction should succeed");

    assert_eq!(result, 42);
    assert_eq!(metrics.transaction.retries, 0);
    assert!(metrics.conflicting_keys.is_empty());
    assert!(
        metrics.transaction.versionstamp.is_some(),
        "write transaction should have a versionstamp"
    );
    let vs = metrics.transaction.versionstamp.unwrap();
    assert_ne!(vs, [0u8; 10], "versionstamp should be non-zero");

    Ok(())
}

/// Conflict path via instrumented_run: force a conflict and verify
/// MetricsReport.conflicting_keys is populated when ReportConflictingKeys is enabled.
///
/// ReportConflictingKeys (option 712) was added in FDB 6.3.
#[cfg_api_versions(min = 630)]
async fn test_conflict_reports_in_metrics() -> FdbResult<()> {
    let db = common::database().await?;
    let attempt = Arc::new(AtomicU64::new(0));

    let (_, metrics) = db
        .instrumented_run(|trx, _| {
            let attempt = attempt.clone();
            async move {
                let current = attempt.fetch_add(1, Ordering::SeqCst);

                // Enable conflict reporting
                trx.set_option(options::TransactionOption::ReportConflictingKeys)?;

                // Read a key to establish read conflict range
                let _ = trx.get(b"test_conflict_metrics_key", false).await?;

                if current == 0 {
                    // On first attempt, write the same key from another transaction
                    let db2 = Database::new_compat(None).await?;
                    let other_trx = db2.create_trx()?;
                    other_trx.set(b"test_conflict_metrics_key", b"other_value");
                    other_trx
                        .commit()
                        .await
                        .map_err(|e| FdbBindingError::NonRetryableFdbError(FdbError::from(e)))?;
                }

                trx.set(b"test_conflict_metrics_key", b"my_value");
                Ok(())
            }
        })
        .await
        .expect("transaction should eventually succeed");

    // Should have retried at least once
    assert!(metrics.transaction.retries >= 1);
    // Should have recorded at least one conflict
    assert!(metrics.transaction.conflict_count >= 1);
    // Conflicting keys should be populated from the first (failed) attempt
    assert!(
        !metrics.conflicting_keys.is_empty(),
        "expected conflicting keys to be reported"
    );

    Ok(())
}

/// Direct API: use Transaction::conflicting_keys() on a TransactionCommitError.
///
/// ReportConflictingKeys (option 712) was added in FDB 6.3.
#[cfg_api_versions(min = 630)]
async fn test_conflict_keys_direct_api() -> FdbResult<()> {
    let db = common::database().await?;

    // Transaction A: read, then try to commit after B writes the same key
    let trx_a = db.create_trx()?;
    trx_a.set_option(options::TransactionOption::ReportConflictingKeys)?;
    let _ = trx_a.get(b"test_conflict_direct_key", false).await?;

    // Transaction B: write the same key and commit
    let trx_b = db.create_trx()?;
    trx_b.set(b"test_conflict_direct_key", b"b_value");
    trx_b.commit().await.expect("trx B should commit");

    // Transaction A: write something and try to commit — should conflict
    trx_a.set(b"test_conflict_direct_key", b"a_value");
    let commit_result = trx_a.commit().await;

    match commit_result {
        Ok(_committed) => {
            // It's possible (though unlikely) that A commits successfully
            // if the read version window doesn't overlap. That's fine.
        }
        Err(commit_error) => {
            // Read conflicting keys before on_error resets the transaction
            let conflicting = commit_error.conflicting_keys().await?;
            for range in &conflicting {
                eprintln!(
                    "conflicting range: begin={:?} end={:?}",
                    String::from_utf8_lossy(range.begin()),
                    String::from_utf8_lossy(range.end()),
                );
            }
            assert!(
                !conflicting.is_empty(),
                "expected conflicting keys on commit error"
            );

            // Verify the conflicting range includes our key
            let key: &[u8] = b"test_conflict_direct_key";
            let has_our_key = conflicting
                .iter()
                .any(|range| key >= range.begin() && key < range.end());
            assert!(has_our_key, "conflicting range should contain our key");
        }
    }

    Ok(())
}

/// Direct API: call get_versionstamp() before commit, await after commit succeeds.
async fn test_versionstamp_direct_api() -> FdbResult<()> {
    let db = common::database().await?;

    let trx = db.create_trx()?;
    trx.set(b"test_versionstamp_direct", b"value");
    // The FDB C API requires get_versionstamp to be called BEFORE commit.
    let vs_future = trx.get_versionstamp();
    trx.commit().await?;

    let vs_slice = vs_future
        .await
        .expect("get_versionstamp should succeed for write transaction");
    assert_eq!(
        vs_slice.len(),
        10,
        "versionstamp should be exactly 10 bytes"
    );
    assert_ne!(&vs_slice[..], &[0u8; 10], "versionstamp should be non-zero");

    Ok(())
}

/// A test-only RunnerHooks implementation that tracks whether on_versionstamp was called.
struct SpyHooks {
    /// Controls whether the hook opts-in to versionstamp resolution.
    wants_vs: bool,
    /// Set to true when on_versionstamp is called.
    versionstamp_called: Arc<AtomicBool>,
}

impl SpyHooks {
    fn new(wants_vs: bool) -> Self {
        Self {
            wants_vs,
            versionstamp_called: Arc::new(AtomicBool::new(false)),
        }
    }

    fn was_versionstamp_called(&self) -> bool {
        self.versionstamp_called.load(Ordering::SeqCst)
    }
}

impl RunnerHooks for SpyHooks {
    fn wants_versionstamp(&self) -> bool {
        self.wants_vs
    }

    fn on_versionstamp(&self, _vs: [u8; 10]) {
        self.versionstamp_called.store(true, Ordering::SeqCst);
    }
}

/// When wants_versionstamp() returns false, on_versionstamp must NOT be called.
async fn test_versionstamp_not_resolved_when_not_wanted() -> FdbResult<()> {
    let db = common::database().await?;
    let spy = SpyHooks::new(false);

    db.run_with_hooks(&spy, |trx, _| async move {
        trx.set(b"test_spy_hooks_no_vs", b"value");
        Ok(())
    })
    .await
    .expect("transaction should succeed");

    assert!(
        !spy.was_versionstamp_called(),
        "on_versionstamp should NOT be called when wants_versionstamp() returns false"
    );

    Ok(())
}

/// When wants_versionstamp() returns true, on_versionstamp MUST be called for a write transaction.
async fn test_versionstamp_resolved_when_wanted() -> FdbResult<()> {
    let db = common::database().await?;
    let spy = SpyHooks::new(true);

    db.run_with_hooks(&spy, |trx, _| async move {
        trx.set(b"test_spy_hooks_with_vs", b"value");
        Ok(())
    })
    .await
    .expect("transaction should succeed");

    assert!(
        spy.was_versionstamp_called(),
        "on_versionstamp MUST be called when wants_versionstamp() returns true"
    );

    Ok(())
}
