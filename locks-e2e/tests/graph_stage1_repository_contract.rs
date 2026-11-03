use std::fs;
use std::path::PathBuf;

#[test]
fn graph_stage1_docs_compose_and_ci_remain_one_reproducible_contract() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_owned();
    let compose = fs::read_to_string(root.join("compose.graph-stage1-e2e.yaml")).unwrap();
    let runner = fs::read_to_string(root.join("scripts/test-graph-stage1-compose.sh")).unwrap();
    let fake = fs::read_to_string(root.join("scripts/graph-e2e-fake-dependencies.py")).unwrap();
    let docs = fs::read_to_string(root.join("docs/GRAPH_STAGE1.md")).unwrap();
    let adr =
        fs::read_to_string(root.join("docs/ADRs/0021-graph-stage1-folded-auth-nexus.md")).unwrap();
    let ci = fs::read_to_string(root.join(".github/workflows/check.yml")).unwrap();

    let pinned_images = [
        "postgres:16-alpine@sha256:0cefaabd882e42bcb2a2f3dc56d3ce4770560ba62fd7f197f1a2e008fd5731ab",
        "python:3.13-alpine@sha256:7984cf56ceaddebe4fefeb75d64000b9f45d8b77b11a09b24524da6cdc3af0c8",
        "rust:1.91.1-bookworm@sha256:c1e5f19e773b7878c3f7a805dd00a495e747acbdc76fb2337a4ebf0418896b33",
    ];
    for required in [
        "graph-e2e-contract-v1",
        "network_mode: service:fake-graph-dependencies",
        "TEST_DATABASE_URL",
        "GRAPH_FAKE_DEPENDENCY_URL",
        "GRAPH_FAKE_NEXUS_URL",
    ] {
        assert!(compose.contains(required), "Compose lost {required}");
    }
    for image in pinned_images {
        assert!(
            compose.contains(image),
            "Compose image is not pinned: {image}"
        );
        assert!(
            docs.contains(image),
            "docs lost pinned image receipt: {image}"
        );
    }
    assert!(!compose.contains("image: postgres:16-alpine\n"));
    assert!(!compose.contains("image: python:3.13-alpine\n"));
    assert!(!compose.contains("image: rust:1.91.1-bookworm\n"));

    let lock_receipt = "sha256sum Cargo.lock examples/js-sdk/package-lock.json";
    assert!(docs.contains(lock_receipt));
    assert!(!docs.contains("2>/dev/null"));
    for path in ["Cargo.lock", "examples/js-sdk/package-lock.json"] {
        assert!(
            root.join(path).is_file(),
            "documented lockfile is absent: {path}"
        );
    }

    assert!(runner.contains("--abort-on-container-exit"));
    assert!(runner.contains("--exit-code-from graph-e2e"));
    for required in [
        "auth_mode",
        "nexus_mode",
        "paykit_calls",
        "last_nexus_path",
        "malformed",
        "unavailable",
        "timeout",
    ] {
        assert!(fake.contains(required), "fake dependency lost {required}");
    }
    for required in [
        "app_spec_v0_7",
        "restart_required",
        "encrypted",
        "zero Paykit",
        "Stage 2",
        "Stage 3",
        "submitProofBundle",
        "pollFoldedAuth",
        "lookupGraphVerificationTask",
        "verificationEvidence",
    ] {
        assert!(docs.contains(required), "graph docs lost {required}");
    }
    for required in ["D1", "D2", "D3", "attempted", "claim", "redaction"] {
        assert!(adr.contains(required), "graph ADR lost {required}");
    }
    assert_eq!(
        ci.matches("./scripts/test-graph-stage1-compose.sh").count(),
        2,
        "CI must prove a repeated clean Compose run"
    );
}
