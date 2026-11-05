use std::collections::BTreeMap;
use std::str::FromStr;

use locks_core::ids::{
    BundleId, CreatorPubky, GuardedResourceHash, LockServerPubky, PubkyLockResource,
};
use locks_core::lock_policy::{
    AccessPolicy, CONTENT_LOCK_V2_VERSION, CONTENT_LOCK_VERSION, ContentLock, ContentLockPayload,
    ContentLockV2, Criterion, GraphOperatorV2, GraphParticipantV2, GraphPolicyV2, GraphPredicateV2,
    GuardedResource, LockLogic, LockServerConfig, PaymentPolicyV2, VerifierType,
};
use locks_core::verification::{
    EntitlementLifetime, Proof, SUBMITTED_PROOF_BUNDLE_V2_VERSION, SUBMITTED_PROOF_BUNDLE_VERSION,
    SatisfiedByV2, SubmittedProofBundle, SubmittedProofBundlePayload, SubmittedProofBundleV2,
    VERIFIED_PROOF_BUNDLE_V2_VERSION, VERIFIED_PROOF_BUNDLE_VERSION, VerificationResult,
    VerifiedProofBundle, VerifiedProofBundlePayload, VerifiedProofBundleV2,
    validate_content_lock_submission, validate_verified_content_lock_publication,
};
use serde_json::{Value, json};
use time::macros::datetime;

const CREATOR: &str = "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy";
const READER: &str = "pubkyorhzqdiexwmi6iidktucgud63ufa5nwtsuzdxe176a8izd6jsqky";
const LOCK_SERVER: &str = "pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo";
const BUNDLE_ID: &str = "000G40R40M30E209185GR38E1W";
const LOCK_ID: &str = "000G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG";
const V2_JSON: &str = r#"{"version":2,"creator":"pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy","primary_resource":{"path":"/priv/app.locks/content/post.json","hash":"0W3GE1R70W3GE1R70W3GE1R70W3GE1R70W3GE1R70W3GE1R70W3G","content_type":"application/json","size":5},"payment":{"recipient_pubky":"pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy","amount":"50000","asset":"BTC","minimum_confirmations":0},"graph":{"predicates":[{"type":"follows","follower":"reader","followed":"creator"},{"type":"tagged","tagger":"reader","tagged":"creator","label":"trusted"}],"operator":"and"},"verification_window_seconds":300,"access_policy":{"requested_credential_ttl_seconds":900},"lock_server":{"override":"pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo"},"created_at":"2026-05-29T12:00:00Z"}"#;
const V2_JCS: &str = r#"{"access_policy":{"requested_credential_ttl_seconds":900},"created_at":"2026-05-29T12:00:00Z","creator":"pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy","graph":{"operator":"and","predicates":[{"followed":"creator","follower":"reader","type":"follows"},{"label":"trusted","tagged":"creator","tagger":"reader","type":"tagged"}]},"lock_server":{"override":"pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo"},"payment":{"amount":"50000","asset":"BTC","minimum_confirmations":0,"recipient_pubky":"pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy"},"primary_resource":{"content_type":"application/json","hash":"0W3GE1R70W3GE1R70W3GE1R70W3GE1R70W3GE1R70W3GE1R70W3G","path":"/priv/app.locks/content/post.json","size":5},"verification_window_seconds":300,"version":2}"#;
const V2_LOCK_ID: &str = "0D8J413YQ94KFWPVDNR6QMR1BD1FSQEW945Y1999ANJCTXDKDFFG";
const V2_PUBLICATION: &str = r#"{"version":2,"creator":"pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy","reader":"pubkyorhzqdiexwmi6iidktucgud63ufa5nwtsuzdxe176a8izd6jsqky","bundle_id":"000G40R40M30E209185GR38E1W","pubky_lock_resource":"pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy/pub/app.locks/0D8J413YQ94KFWPVDNR6QMR1BD1FSQEW945Y1999ANJCTXDKDFFG.json","verified_at":"2026-05-29T12:30:00Z","satisfied_by":["payment","zero_conf_graph"]}"#;
const V1_JSON: &str = r#"{"version":1,"creator":"pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy","primary_resource":{"path":"/priv/app.locks/content/post.json","hash":"0W3GE1R70W3GE1R70W3GE1R70W3GE1R70W3GE1R70W3GE1R70W3G","content_type":"application/json","size":5},"criteria":[{"criterion_id":"criterion-1","verifier_type":"dev-static","params":{"satisfied":true}}],"lock_logic":{"type":"all","criteria":["criterion-1"]},"access_policy":{"requested_credential_ttl_seconds":900},"lock_server":{"override":"pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo"},"created_at":"2026-05-29T12:00:00Z"}"#;
const V1_JCS: &str = r#"{"access_policy":{"requested_credential_ttl_seconds":900},"created_at":"2026-05-29T12:00:00Z","creator":"pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy","criteria":[{"criterion_id":"criterion-1","params":{"satisfied":true},"verifier_type":"dev-static"}],"lock_logic":{"criteria":["criterion-1"],"type":"all"},"lock_server":{"override":"pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo"},"primary_resource":{"content_type":"application/json","hash":"0W3GE1R70W3GE1R70W3GE1R70W3GE1R70W3GE1R70W3GE1R70W3G","path":"/priv/app.locks/content/post.json","size":5},"version":1}"#;
const V1_LOCK_ID: &str = "PQVYQ2AY7XCMR7GZHM0NZTRQK9RW92G8T3QDMWAEPS1BVSGFB7NG";

fn creator() -> CreatorPubky {
    CreatorPubky::from_str(CREATOR).unwrap()
}

fn reader() -> CreatorPubky {
    CreatorPubky::from_str(READER).unwrap()
}

fn primary_resource() -> GuardedResource {
    GuardedResource {
        path: "/priv/app.locks/content/post.json".to_owned(),
        hash: GuardedResourceHash::from_bytes([7; 32]),
        content_type: "application/json".to_owned(),
        size: 5,
    }
}

fn payment(confirmations: u8) -> PaymentPolicyV2 {
    PaymentPolicyV2 {
        recipient_pubky: creator(),
        amount: "50000".to_owned(),
        asset: "BTC".to_owned(),
        minimum_confirmations: confirmations,
    }
}

fn follows() -> GraphPredicateV2 {
    GraphPredicateV2::Follows {
        follower: GraphParticipantV2::Reader,
        followed: GraphParticipantV2::Creator,
    }
}

fn tagged(label: &str) -> GraphPredicateV2 {
    GraphPredicateV2::Tagged {
        tagger: GraphParticipantV2::Reader,
        tagged: GraphParticipantV2::Creator,
        label: label.to_owned(),
    }
}

fn graph(predicates: Vec<GraphPredicateV2>, operator: Option<GraphOperatorV2>) -> GraphPolicyV2 {
    GraphPolicyV2 {
        predicates,
        operator,
    }
}

fn content_lock_v2(
    payment: Option<PaymentPolicyV2>,
    graph: Option<GraphPolicyV2>,
) -> ContentLockV2 {
    ContentLockV2 {
        version: CONTENT_LOCK_V2_VERSION,
        creator: creator(),
        primary_resource: Some(primary_resource()),
        secondary_resources: BTreeMap::new(),
        payment,
        graph,
        verification_window_seconds: 300,
        access_policy: AccessPolicy {
            requested_credential_ttl_seconds: 900,
        },
        lock_server: LockServerConfig {
            override_: Some(LockServerPubky::from_str(LOCK_SERVER).unwrap()),
        },
        created_at: datetime!(2026-05-29 12:00:00 UTC),
    }
}

fn content_lock_v1() -> ContentLock {
    ContentLock {
        version: CONTENT_LOCK_VERSION,
        creator: creator(),
        primary_resource: Some(primary_resource()),
        secondary_resources: BTreeMap::new(),
        criteria: vec![Criterion {
            criterion_id: "criterion-1".to_owned(),
            verifier_type: VerifierType::DevStatic,
            params: json!({"satisfied": true}),
        }],
        lock_logic: LockLogic::All {
            criteria: vec!["criterion-1".to_owned()],
        },
        access_policy: AccessPolicy {
            requested_credential_ttl_seconds: 900,
        },
        lock_server: LockServerConfig {
            override_: Some(LockServerPubky::from_str(LOCK_SERVER).unwrap()),
        },
        created_at: datetime!(2026-05-29 12:00:00 UTC),
    }
}

fn lock_resource() -> PubkyLockResource {
    PubkyLockResource::from_str(&format!("{CREATOR}/pub/app.locks/{LOCK_ID}.json")).unwrap()
}

fn lock_resource_for_v1(lock: &ContentLock) -> PubkyLockResource {
    PubkyLockResource::from_str(&format!(
        "{CREATOR}/pub/app.locks/{}.json",
        lock.lock_id().unwrap()
    ))
    .unwrap()
}

fn lock_resource_for_v2(lock: &ContentLockV2) -> PubkyLockResource {
    PubkyLockResource::from_str(&format!(
        "{CREATOR}/pub/app.locks/{}.json",
        lock.lock_id().unwrap()
    ))
    .unwrap()
}

fn submitted_v1(lock: &ContentLock) -> SubmittedProofBundle {
    SubmittedProofBundle {
        version: SUBMITTED_PROOF_BUNDLE_VERSION,
        bundle_id: BundleId::from_str(BUNDLE_ID).unwrap(),
        pubky_lock_resource: lock_resource_for_v1(lock),
        reader_public_key: Some(reader()),
        client_reference: None,
        proofs: vec![Proof {
            criterion_id: "criterion-1".to_owned(),
            verifier_type: VerifierType::DevStatic,
            payload: json!({"answer": "viewer-supplied"}),
        }],
    }
}

fn submitted_v2(
    lock: &ContentLockV2,
    reader_pubky: Option<CreatorPubky>,
) -> SubmittedProofBundleV2 {
    SubmittedProofBundleV2 {
        version: SUBMITTED_PROOF_BUNDLE_V2_VERSION,
        bundle_id: BundleId::from_str(BUNDLE_ID).unwrap(),
        pubky_lock_resource: lock_resource_for_v2(lock),
        reader_pubky,
    }
}

#[test]
fn versioned_content_lock_dispatch_is_closed() {
    let v1 = serde_json::to_value(content_lock_v1()).unwrap();
    let v2 = serde_json::to_value(content_lock_v2(Some(payment(0)), None)).unwrap();

    assert!(matches!(
        serde_json::from_value::<ContentLockPayload>(v1.clone()).unwrap(),
        ContentLockPayload::V1(_)
    ));
    assert!(matches!(
        serde_json::from_value::<ContentLockPayload>(v2.clone()).unwrap(),
        ContentLockPayload::V2(_)
    ));

    for mut invalid in [v1, v2] {
        invalid["version"] = json!(3);
        assert!(serde_json::from_value::<ContentLockPayload>(invalid).is_err());
    }

    let mut v2_with_v1_fields =
        serde_json::to_value(content_lock_v2(Some(payment(0)), None)).unwrap();
    v2_with_v1_fields["criteria"] = json!([]);
    assert!(serde_json::from_value::<ContentLockPayload>(v2_with_v1_fields).is_err());
}

#[test]
fn versioned_dispatch_rejects_duplicate_top_level_and_nested_fields() {
    let v1_lock = content_lock_v1();
    let v1_lock_json = serde_json::to_string(&v1_lock).unwrap();
    let duplicate_v1_lock_version =
        v1_lock_json.replacen("\"version\":1", "\"version\":1,\"version\":1", 1);
    assert!(serde_json::from_str::<ContentLockPayload>(&duplicate_v1_lock_version).is_err());

    let v1_submitted_json = serde_json::to_string(&submitted_v1(&v1_lock)).unwrap();
    let duplicate_v1_submitted_version =
        v1_submitted_json.replacen("\"version\":1", "\"version\":1,\"version\":1", 1);
    assert!(
        serde_json::from_str::<SubmittedProofBundlePayload>(&duplicate_v1_submitted_version)
            .is_err()
    );

    let v1_verified = VerifiedProofBundle {
        version: VERIFIED_PROOF_BUNDLE_VERSION,
        bundle_id: BundleId::from_str(BUNDLE_ID).unwrap(),
        pubky_lock_resource: lock_resource_for_v1(&v1_lock),
        verification_result: VerificationResult { criteria: vec![] },
        entitlement_lifetime: EntitlementLifetime::Unbounded,
    };
    let v1_verified_json = serde_json::to_string(&v1_verified).unwrap();
    let duplicate_v1_verified_version =
        v1_verified_json.replacen("\"version\":1", "\"version\":1,\"version\":1", 1);
    assert!(
        serde_json::from_str::<VerifiedProofBundlePayload>(&duplicate_v1_verified_version).is_err()
    );

    let lock = content_lock_v2(Some(payment(0)), None);
    let lock_json = serde_json::to_string(&lock).unwrap();
    let duplicate_lock_version =
        lock_json.replacen("\"version\":2", "\"version\":2,\"version\":2", 1);
    let duplicate_payment_amount = lock_json.replacen(
        "\"amount\":\"50000\"",
        "\"amount\":\"50000\",\"amount\":\"50000\"",
        1,
    );
    assert!(serde_json::from_str::<ContentLockPayload>(&duplicate_lock_version).is_err());
    assert!(serde_json::from_str::<ContentLockPayload>(&duplicate_payment_amount).is_err());

    let submitted_json = serde_json::to_string(&submitted_v2(&lock, Some(reader()))).unwrap();
    let duplicate_submitted_version =
        submitted_json.replacen("\"version\":2", "\"version\":2,\"version\":2", 1);
    assert!(
        serde_json::from_str::<SubmittedProofBundlePayload>(&duplicate_submitted_version).is_err()
    );

    let verified = verified_v2(&lock, vec![SatisfiedByV2::Payment]);
    let verified_json = serde_json::to_string(&verified).unwrap();
    let duplicate_verified_version =
        verified_json.replacen("\"version\":2", "\"version\":2,\"version\":2", 1);
    assert!(
        serde_json::from_str::<VerifiedProofBundlePayload>(&duplicate_verified_version).is_err()
    );
}

#[test]
fn versioned_dispatch_rejects_malformed_numbers() {
    let lock = content_lock_v2(Some(payment(0)), None);
    let lock_json = serde_json::to_string(&lock).unwrap();
    for malformed in [
        lock_json.replacen("\"version\":2", "\"version\":-1", 1),
        lock_json.replacen("\"version\":2", "\"version\":2.5", 1),
        lock_json.replacen("\"version\":2", "\"version\":18446744073709551616", 1),
        lock_json.replacen(
            "\"minimum_confirmations\":0",
            "\"minimum_confirmations\":-1",
            1,
        ),
        lock_json.replacen(
            "\"minimum_confirmations\":0",
            "\"minimum_confirmations\":1.5",
            1,
        ),
        lock_json.replacen(
            "\"verification_window_seconds\":300",
            "\"verification_window_seconds\":1e400",
            1,
        ),
    ] {
        assert!(
            serde_json::from_str::<ContentLockPayload>(&malformed).is_err(),
            "accepted malformed numeric payload: {malformed}"
        );
    }

    let submitted_json = serde_json::to_string(&submitted_v2(&lock, Some(reader()))).unwrap();
    for malformed in [
        submitted_json.replacen("\"version\":2", "\"version\":-1", 1),
        submitted_json.replacen("\"version\":2", "\"version\":2.5", 1),
    ] {
        assert!(serde_json::from_str::<SubmittedProofBundlePayload>(&malformed).is_err());
    }
}

#[test]
fn content_lock_v2_presence_modes_and_payment_bounds_are_closed() {
    for lock in [
        content_lock_v2(Some(payment(0)), None),
        content_lock_v2(None, Some(graph(vec![follows()], None))),
        content_lock_v2(
            Some(payment(6)),
            Some(graph(
                vec![follows(), tagged("trusted")],
                Some(GraphOperatorV2::Or),
            )),
        ),
    ] {
        let value = serde_json::to_value(lock).unwrap();
        assert!(serde_json::from_value::<ContentLockV2>(value).is_ok());
    }

    for lock in [
        content_lock_v2(None, None),
        content_lock_v2(Some(payment(7)), None),
    ] {
        let value = serde_json::to_value(lock).unwrap();
        assert!(serde_json::from_value::<ContentLockV2>(value).is_err());
    }
}

#[test]
fn payment_v2_requires_exact_btc_and_u64_amount_without_normalizing_lexeme() {
    for amount in ["1", "0001", "18446744073709551615"] {
        let mut policy = payment(0);
        policy.amount = amount.to_owned();
        let lock = content_lock_v2(Some(policy), None);
        let decoded = serde_json::from_value::<ContentLockV2>(serde_json::to_value(lock).unwrap())
            .expect("valid bounded BTC amount");
        assert_eq!(decoded.payment.unwrap().amount, amount);
    }

    for asset in ["btc", "USDT", ""] {
        let mut policy = payment(0);
        policy.asset = asset.to_owned();
        let lock = content_lock_v2(Some(policy), None);
        assert!(
            serde_json::from_value::<ContentLockV2>(serde_json::to_value(lock).unwrap()).is_err()
        );
    }

    for amount in ["0", "000", "1.0", "+1", "18446744073709551616"] {
        let mut policy = payment(0);
        policy.amount = amount.to_owned();
        let lock = content_lock_v2(Some(policy), None);
        assert!(
            serde_json::from_value::<ContentLockV2>(serde_json::to_value(lock).unwrap()).is_err()
        );
    }
}

#[test]
fn graph_v2_cardinality_operator_duplicates_and_order_are_closed() {
    let one = content_lock_v2(None, Some(graph(vec![follows()], None)));
    assert!(serde_json::from_value::<ContentLockV2>(serde_json::to_value(one).unwrap()).is_ok());

    let invalid_graphs = [
        graph(vec![], None),
        graph(vec![follows()], Some(GraphOperatorV2::And)),
        graph(vec![follows(), tagged("trusted")], None),
        graph(vec![follows(); 9], Some(GraphOperatorV2::Or)),
        graph(vec![follows(), follows()], Some(GraphOperatorV2::And)),
    ];
    for graph in invalid_graphs {
        let value = serde_json::to_value(content_lock_v2(None, Some(graph))).unwrap();
        assert!(serde_json::from_value::<ContentLockV2>(value).is_err());
    }

    let first = content_lock_v2(
        None,
        Some(graph(
            vec![follows(), tagged("trusted")],
            Some(GraphOperatorV2::And),
        )),
    );
    let second = content_lock_v2(
        None,
        Some(graph(
            vec![tagged("trusted"), follows()],
            Some(GraphOperatorV2::And),
        )),
    );
    assert_ne!(
        first.canonical_json_string().unwrap(),
        second.canonical_json_string().unwrap()
    );
    assert_ne!(first.lock_id().unwrap(), second.lock_id().unwrap());
}

#[test]
fn graph_v2_roles_and_labels_are_closed() {
    let invalid_predicates = [
        GraphPredicateV2::Follows {
            follower: GraphParticipantV2::Creator,
            followed: GraphParticipantV2::Creator,
        },
        GraphPredicateV2::Tagged {
            tagger: GraphParticipantV2::Reader,
            tagged: GraphParticipantV2::Reader,
            label: "trusted".to_owned(),
        },
        tagged(""),
        tagged("Trusted"),
        tagged(" trusted"),
        tagged("has space"),
        tagged("has,comma"),
        tagged("has:colon"),
        tagged("abcdefghijklmnopqrstu"),
    ];

    for predicate in invalid_predicates {
        let value = serde_json::to_value(content_lock_v2(None, Some(graph(vec![predicate], None))))
            .unwrap();
        assert!(serde_json::from_value::<ContentLockV2>(value).is_err());
    }

    for label in ["trusted", "é", "安全"] {
        let value = serde_json::to_value(content_lock_v2(
            None,
            Some(graph(vec![tagged(label)], None)),
        ))
        .unwrap();
        assert!(
            serde_json::from_value::<ContentLockV2>(value).is_ok(),
            "{label}"
        );
    }
}

#[test]
fn submitted_v2_is_closed_and_reader_presence_matches_lock_mode() {
    let payment_lock = content_lock_v2(Some(payment(0)), None);
    let graph_lock = content_lock_v2(None, Some(graph(vec![follows()], None)));
    let mixed_lock = content_lock_v2(Some(payment(6)), Some(graph(vec![follows()], None)));

    assert!(
        validate_content_lock_submission(
            &ContentLockPayload::V2(payment_lock.clone()),
            &SubmittedProofBundlePayload::V2(submitted_v2(&payment_lock, Some(reader())))
        )
        .is_ok()
    );
    assert!(
        validate_content_lock_submission(
            &ContentLockPayload::V2(payment_lock.clone()),
            &SubmittedProofBundlePayload::V2(submitted_v2(&payment_lock, None))
        )
        .is_err()
    );

    for lock in [graph_lock, mixed_lock] {
        assert!(
            validate_content_lock_submission(
                &ContentLockPayload::V2(lock.clone()),
                &SubmittedProofBundlePayload::V2(submitted_v2(&lock, None))
            )
            .is_ok()
        );
        assert!(
            validate_content_lock_submission(
                &ContentLockPayload::V2(lock.clone()),
                &SubmittedProofBundlePayload::V2(submitted_v2(&lock, Some(reader())))
            )
            .is_err()
        );
    }

    let mut value = serde_json::to_value(submitted_v2(&payment_lock, None)).unwrap();
    value["proofs"] = json!([]);
    assert!(serde_json::from_value::<SubmittedProofBundleV2>(value).is_err());
}

#[test]
fn strict_version_pairing_rejects_cross_version_pairs() {
    let v1 = content_lock_v1();
    let v2 = content_lock_v2(Some(payment(0)), None);
    let v1_lock = ContentLockPayload::V1(v1.clone());
    let v2_lock = ContentLockPayload::V2(v2.clone());
    let v1_bundle = SubmittedProofBundlePayload::V1(submitted_v1(&v1));
    let v2_bundle = SubmittedProofBundlePayload::V2(submitted_v2(&v2, Some(reader())));

    assert!(validate_content_lock_submission(&v1_lock, &v1_bundle).is_ok());
    assert!(validate_content_lock_submission(&v2_lock, &v2_bundle).is_ok());
    assert!(validate_content_lock_submission(&v1_lock, &v2_bundle).is_err());
    assert!(validate_content_lock_submission(&v2_lock, &v1_bundle).is_err());

    for version in [0, 3, u16::MAX] {
        let mut value = serde_json::to_value(submitted_v2(&v2, None)).unwrap();
        value["version"] = json!(version);
        assert!(serde_json::from_value::<SubmittedProofBundlePayload>(value).is_err());
    }
}

#[test]
fn submissions_bind_resource_creator_and_lock_id_for_both_versions() {
    let v1 = content_lock_v1();
    let valid_v1 = submitted_v1(&v1);
    assert!(
        validate_content_lock_submission(
            &ContentLockPayload::V1(v1.clone()),
            &SubmittedProofBundlePayload::V1(valid_v1.clone()),
        )
        .is_ok()
    );

    let v2 = content_lock_v2(Some(payment(0)), None);
    let valid_v2 = submitted_v2(&v2, Some(reader()));
    assert!(
        validate_content_lock_submission(
            &ContentLockPayload::V2(v2.clone()),
            &SubmittedProofBundlePayload::V2(valid_v2.clone()),
        )
        .is_ok()
    );

    for mismatched_resource in [
        lock_resource(),
        PubkyLockResource::from_str(&format!(
            "{READER}/pub/app.locks/{}.json",
            v2.lock_id().unwrap()
        ))
        .unwrap(),
    ] {
        let mut invalid = valid_v2.clone();
        invalid.pubky_lock_resource = mismatched_resource;
        assert!(
            validate_content_lock_submission(
                &ContentLockPayload::V2(v2.clone()),
                &SubmittedProofBundlePayload::V2(invalid),
            )
            .is_err()
        );
    }

    for mismatched_resource in [
        lock_resource(),
        PubkyLockResource::from_str(&format!(
            "{READER}/pub/app.locks/{}.json",
            v1.lock_id().unwrap()
        ))
        .unwrap(),
    ] {
        let mut invalid = valid_v1.clone();
        invalid.pubky_lock_resource = mismatched_resource;
        assert!(
            validate_content_lock_submission(
                &ContentLockPayload::V1(v1.clone()),
                &SubmittedProofBundlePayload::V1(invalid),
            )
            .is_err()
        );
    }
}

fn verified_v2(lock: &ContentLockV2, satisfied_by: Vec<SatisfiedByV2>) -> VerifiedProofBundleV2 {
    VerifiedProofBundleV2 {
        version: VERIFIED_PROOF_BUNDLE_V2_VERSION,
        creator: creator(),
        reader: reader(),
        bundle_id: BundleId::from_str(BUNDLE_ID).unwrap(),
        pubky_lock_resource: lock_resource_for_v2(lock),
        verified_at: datetime!(2026-05-29 12:30:00 UTC),
        satisfied_by,
    }
}

#[test]
fn verified_publication_paths_match_lock_mode_and_bind_lock_identity() {
    let payment_lock = content_lock_v2(Some(payment(0)), None);
    let graph_lock = content_lock_v2(None, Some(graph(vec![follows()], None)));
    let mixed_lock = content_lock_v2(Some(payment(0)), Some(graph(vec![follows()], None)));

    let valid = [
        (&payment_lock, vec![SatisfiedByV2::Payment]),
        (&graph_lock, vec![SatisfiedByV2::Graph]),
        (&mixed_lock, vec![SatisfiedByV2::Payment]),
        (&mixed_lock, vec![SatisfiedByV2::ZeroConfGraph]),
        (
            &mixed_lock,
            vec![SatisfiedByV2::Payment, SatisfiedByV2::ZeroConfGraph],
        ),
    ];
    for (lock, paths) in valid {
        assert!(
            validate_verified_content_lock_publication(lock, &verified_v2(lock, paths)).is_ok()
        );
    }

    let invalid = [
        (&payment_lock, vec![SatisfiedByV2::Graph]),
        (&payment_lock, vec![SatisfiedByV2::ZeroConfGraph]),
        (&graph_lock, vec![SatisfiedByV2::Payment]),
        (&graph_lock, vec![SatisfiedByV2::ZeroConfGraph]),
        (&mixed_lock, vec![SatisfiedByV2::Graph]),
    ];
    for (lock, paths) in invalid {
        assert!(
            validate_verified_content_lock_publication(lock, &verified_v2(lock, paths)).is_err()
        );
    }

    let mut wrong_creator = verified_v2(&payment_lock, vec![SatisfiedByV2::Payment]);
    wrong_creator.creator = reader();
    assert!(validate_verified_content_lock_publication(&payment_lock, &wrong_creator).is_err());

    let wrong_lock = verified_v2(&mixed_lock, vec![SatisfiedByV2::Payment]);
    assert!(validate_verified_content_lock_publication(&payment_lock, &wrong_lock).is_err());
}

#[test]
fn verified_v2_has_only_approved_fields_and_canonical_non_empty_satisfied_by() {
    let fixture = VerifiedProofBundleV2 {
        version: VERIFIED_PROOF_BUNDLE_V2_VERSION,
        creator: creator(),
        reader: reader(),
        bundle_id: BundleId::from_str(BUNDLE_ID).unwrap(),
        pubky_lock_resource: lock_resource(),
        verified_at: datetime!(2026-05-29 12:30:00 UTC),
        satisfied_by: vec![SatisfiedByV2::Payment, SatisfiedByV2::ZeroConfGraph],
    };
    let value = serde_json::to_value(&fixture).unwrap();
    assert_eq!(
        value,
        json!({
            "version": 2,
            "creator": CREATOR,
            "reader": READER,
            "bundle_id": BUNDLE_ID,
            "pubky_lock_resource": format!("{CREATOR}/pub/app.locks/{LOCK_ID}.json"),
            "verified_at": "2026-05-29T12:30:00Z",
            "satisfied_by": ["payment", "zero_conf_graph"]
        })
    );
    assert!(serde_json::from_value::<VerifiedProofBundleV2>(value.clone()).is_ok());

    for satisfied_by in [
        json!([]),
        json!(["zero_conf_graph", "payment"]),
        json!(["payment", "payment"]),
        json!(["graph", "payment"]),
        json!(["unknown"]),
    ] {
        let mut invalid = value.clone();
        invalid["satisfied_by"] = satisfied_by;
        assert!(serde_json::from_value::<VerifiedProofBundleV2>(invalid).is_err());
    }

    let mut extra = value;
    extra["payment"] = json!({"private": true});
    assert!(serde_json::from_value::<VerifiedProofBundleV2>(extra).is_err());
}

#[test]
fn versioned_verified_bundle_dispatch_rejects_unknown_versions() {
    let v2 = VerifiedProofBundleV2 {
        version: VERIFIED_PROOF_BUNDLE_V2_VERSION,
        creator: creator(),
        reader: reader(),
        bundle_id: BundleId::from_str(BUNDLE_ID).unwrap(),
        pubky_lock_resource: lock_resource(),
        verified_at: datetime!(2026-05-29 12:30:00 UTC),
        satisfied_by: vec![SatisfiedByV2::Graph],
    };
    assert!(matches!(
        serde_json::from_value::<VerifiedProofBundlePayload>(serde_json::to_value(&v2).unwrap())
            .unwrap(),
        VerifiedProofBundlePayload::V2(_)
    ));

    let mut unknown = serde_json::to_value(v2).unwrap();
    unknown["version"] = json!(3);
    assert!(serde_json::from_value::<VerifiedProofBundlePayload>(unknown).is_err());
}

#[test]
fn content_lock_v2_accepts_exact_confirmation_and_window_ranges() {
    for confirmations in 0..=6 {
        let value =
            serde_json::to_value(content_lock_v2(Some(payment(confirmations)), None)).unwrap();
        assert!(
            serde_json::from_value::<ContentLockV2>(value).is_ok(),
            "confirmations={confirmations}"
        );
    }

    for window in [60, 604_800] {
        let mut lock = content_lock_v2(Some(payment(0)), None);
        lock.verification_window_seconds = window;
        assert!(
            serde_json::from_value::<ContentLockV2>(serde_json::to_value(lock).unwrap()).is_ok()
        );
    }
    for window in [0, 59, 604_801, u64::MAX] {
        let mut lock = content_lock_v2(Some(payment(0)), None);
        lock.verification_window_seconds = window;
        assert!(
            serde_json::from_value::<ContentLockV2>(serde_json::to_value(lock).unwrap()).is_err()
        );
    }
}

#[test]
fn content_lock_v2_rejects_invalid_payment_fields() {
    let invalid_payments = [
        PaymentPolicyV2 {
            recipient_pubky: reader(),
            ..payment(0)
        },
        PaymentPolicyV2 {
            amount: "0".to_owned(),
            ..payment(0)
        },
        PaymentPolicyV2 {
            amount: "0.5".to_owned(),
            ..payment(0)
        },
        PaymentPolicyV2 {
            asset: String::new(),
            ..payment(0)
        },
        PaymentPolicyV2 {
            asset: "BTC\n".to_owned(),
            ..payment(0)
        },
    ];
    for invalid in invalid_payments {
        let value = serde_json::to_value(content_lock_v2(Some(invalid), None)).unwrap();
        assert!(serde_json::from_value::<ContentLockV2>(value).is_err());
    }

    let mut unknown = serde_json::to_value(content_lock_v2(Some(payment(0)), None)).unwrap();
    unknown["payment"]["memo"] = json!("not allowed");
    assert!(serde_json::from_value::<ContentLockV2>(unknown).is_err());
}

#[test]
fn graph_v2_accepts_every_cardinality_with_exact_operator_rule() {
    for cardinality in 1..=8 {
        for operator in [GraphOperatorV2::And, GraphOperatorV2::Or] {
            let predicates = (0..cardinality)
                .map(|index| tagged(&format!("label-{index}")))
                .collect();
            let operator = (cardinality > 1).then_some(operator);
            let value =
                serde_json::to_value(content_lock_v2(None, Some(graph(predicates, operator))))
                    .unwrap();
            assert!(
                serde_json::from_value::<ContentLockV2>(value).is_ok(),
                "cardinality={cardinality}, operator={operator:?}"
            );
        }
    }

    let mut unknown =
        serde_json::to_value(content_lock_v2(None, Some(graph(vec![follows()], None)))).unwrap();
    unknown["graph"]["predicates"][0]["nested"] = json!({"not": "allowed"});
    assert!(serde_json::from_value::<ContentLockV2>(unknown).is_err());
}

#[test]
fn graph_v2_accepts_both_distinct_role_directions() {
    for predicate in [
        GraphPredicateV2::Follows {
            follower: GraphParticipantV2::Creator,
            followed: GraphParticipantV2::Reader,
        },
        GraphPredicateV2::Tagged {
            tagger: GraphParticipantV2::Creator,
            tagged: GraphParticipantV2::Reader,
            label: "trusted".to_owned(),
        },
    ] {
        let value = serde_json::to_value(content_lock_v2(None, Some(graph(vec![predicate], None))))
            .unwrap();
        assert!(serde_json::from_value::<ContentLockV2>(value).is_ok());
    }
}

#[test]
fn submitted_v2_has_fixed_minimal_json_vectors() {
    let payment_lock = content_lock_v2(Some(payment(0)), None);
    let graph_lock = content_lock_v2(None, Some(graph(vec![follows()], None)));
    assert_eq!(
        serde_json::to_string(&submitted_v2(&payment_lock, Some(reader()))).unwrap(),
        format!(
            "{{\"version\":2,\"bundle_id\":\"{BUNDLE_ID}\",\"pubky_lock_resource\":\"{CREATOR}/pub/app.locks/{}.json\",\"reader_pubky\":\"{READER}\"}}",
            payment_lock.lock_id().unwrap()
        )
    );
    assert_eq!(
        serde_json::to_string(&submitted_v2(&graph_lock, None)).unwrap(),
        format!(
            "{{\"version\":2,\"bundle_id\":\"{BUNDLE_ID}\",\"pubky_lock_resource\":\"{CREATOR}/pub/app.locks/{}.json\"}}",
            graph_lock.lock_id().unwrap()
        )
    );
}

#[test]
fn fixed_v2_json_jcs_lock_id_and_publication_vectors() {
    let lock = content_lock_v2(
        Some(payment(0)),
        Some(graph(
            vec![follows(), tagged("trusted")],
            Some(GraphOperatorV2::And),
        )),
    );
    let ordinary_json = serde_json::to_string(&lock).unwrap();
    let canonical_json = lock.canonical_json_string().unwrap();

    let publication = VerifiedProofBundleV2 {
        version: VERIFIED_PROOF_BUNDLE_V2_VERSION,
        creator: creator(),
        reader: reader(),
        bundle_id: BundleId::from_str(BUNDLE_ID).unwrap(),
        pubky_lock_resource: lock_resource_for_v2(&lock),
        verified_at: datetime!(2026-05-29 12:30:00 UTC),
        satisfied_by: vec![SatisfiedByV2::Payment, SatisfiedByV2::ZeroConfGraph],
    };
    assert_eq!(ordinary_json, V2_JSON);
    assert_eq!(canonical_json, V2_JCS);
    assert_eq!(lock.lock_id().unwrap().to_string(), V2_LOCK_ID);
    assert_eq!(serde_json::to_string(&publication).unwrap(), V2_PUBLICATION);
}

#[test]
fn immutable_v1_json_jcs_and_lock_id_vectors() {
    let lock = content_lock_v1();
    assert_eq!(serde_json::to_string(&lock).unwrap(), V1_JSON);
    assert_eq!(lock.canonical_json_string().unwrap(), V1_JCS);
    assert_eq!(lock.lock_id().unwrap().to_string(), V1_LOCK_ID);

    let verified = VerifiedProofBundle {
        version: VERIFIED_PROOF_BUNDLE_VERSION,
        bundle_id: BundleId::from_str(BUNDLE_ID).unwrap(),
        pubky_lock_resource: lock_resource(),
        verification_result: VerificationResult { criteria: vec![] },
        entitlement_lifetime: EntitlementLifetime::Unbounded,
    };
    let value: Value = serde_json::to_value(verified).unwrap();
    assert_eq!(value["version"], json!(1));
    assert!(value.get("creator").is_none());
    assert!(value.get("satisfied_by").is_none());

    assert_eq!(
        serde_json::to_string(&submitted_v1(&lock)).unwrap(),
        format!(
            "{{\"version\":1,\"bundle_id\":\"{BUNDLE_ID}\",\"pubky_lock_resource\":\"{CREATOR}/pub/app.locks/{V1_LOCK_ID}.json\",\"reader_public_key\":\"{READER}\",\"proofs\":[{{\"criterion_id\":\"criterion-1\",\"verifier_type\":\"dev-static\",\"payload\":{{\"answer\":\"viewer-supplied\"}}}}]}}"
        )
    );
}
