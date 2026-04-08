"""Tests for src/innovative_features.py."""

import duckdb

from src.innovative_features import (
    detect_innovative_features,
    detect_ai_mentions,
    INNOVATIVE_PATTERNS,
    AI_MENTION_PATTERNS,
)


def _setup_features_test_db():
    """Create an in-memory DuckDB with mock data for innovative feature detection."""
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA raw")
    conn.execute("CREATE SCHEMA class")

    conn.execute("""
        CREATE TABLE raw.studies AS
        SELECT * FROM (VALUES
            ('NCT001', 'An Adaptive Phase II Trial of Drug X', 'An Adaptive Randomized Phase II Trial of Drug X for NSCLC', 'INTERVENTIONAL'),
            ('NCT002', 'Basket Trial for Solid Tumors', 'A Basket Trial Evaluating Drug Y Across Tumor Types', 'INTERVENTIONAL'),
            ('NCT003', 'Umbrella Study in Breast Cancer', 'An Umbrella Study of Multiple Therapies in Breast Cancer', 'INTERVENTIONAL'),
            ('NCT004', 'A Platform Trial for COVID-19', 'A Platform Trial Evaluating Multiple Therapeutics for COVID-19', 'INTERVENTIONAL'),
            ('NCT005', 'A Standard Phase III Trial', 'A Standard Randomized Double-Blind Phase III Trial of Drug W', 'INTERVENTIONAL'),
            ('NCT006', 'Adaptive Behavior Therapy in Children', 'Evaluation of Adaptive Behavior Interventions in Autism', 'INTERVENTIONAL'),
            ('NCT007', 'A SMART Design for Smoking Cessation', 'A Sequential Multiple Assignment Randomized Trial for Cessation', 'INTERVENTIONAL'),
            ('NCT008', 'Pragmatic Trial of Aspirin', 'A Pragmatic Randomized Trial of Aspirin in Primary Care', 'INTERVENTIONAL'),
            ('NCT009', 'Nutritional Enrichment Program', 'A Program of Nutritional Enrichment in Low Birth Weight Infants', 'INTERVENTIONAL'),
            ('NCT010', 'Enrichment Design in Oncology', 'An Enrichment Design Study for Biomarker-Positive Patients', 'INTERVENTIONAL'),
            ('NCT011', 'Digital Health Platform', 'Evaluation of a Digital Health Platform for Diabetes Management', 'INTERVENTIONAL'),
            ('NCT012', 'A Seamless Phase II/III Study', 'A Seamless Phase II/III Adaptive Study of Drug Z', 'INTERVENTIONAL'),
            ('NCT013', 'Master Protocol for Rare Diseases', 'A Master Protocol Evaluating Treatments for Rare Diseases', 'INTERVENTIONAL'),
            ('NCT014', 'N-of-1 Trial Design', 'An N-of-1 Crossover Trial of Analgesics', 'INTERVENTIONAL'),
            ('NCT015', 'Digital Twin for Stroke Prognosis', 'Development of a Digital Twin Model for Stroke Outcome Prediction', 'OBSERVATIONAL'),
            ('NCT016', 'In Silico Trial of Drug Q', 'An In Silico Trial Simulation of Drug Q Pharmacokinetics', 'INTERVENTIONAL'),
            ('NCT017', 'AI-Driven Colonoscopy Screening', 'AI-Driven Screening Tool for Colorectal Cancer Detection', 'INTERVENTIONAL'),
            ('NCT018', 'AI-Guided Trial Design', 'An AI-Guided Trial Design for Adaptive Dosing in Oncology', 'INTERVENTIONAL'),
            ('NCT019', 'Machine Learning Sepsis Prediction', 'A Machine Learning Model to Predict Sepsis in ICU Patients', 'OBSERVATIONAL'),
            ('NCT020', 'Standard Drug Study', 'A Randomized Study of Drug R vs Placebo', 'INTERVENTIONAL')
        ) AS t(nct_id, brief_title, official_title, study_type)
    """)

    conn.execute("""
        CREATE TABLE raw.detailed_descriptions AS
        SELECT * FROM (VALUES
            ('NCT001', 'This is an adaptive design trial with interim analyses.'),
            ('NCT005', 'This is a standard randomized controlled trial with conventional design.'),
            ('NCT007', 'This study uses a SMART design to optimize treatment sequences.'),
            ('NCT012', 'This is a seamless phase 2/3 study with bayesian monitoring.'),
            ('NCT015', 'This study develops a digital twin of patient physiology for personalized stroke care.'),
            ('NCT017', 'This trial uses artificial intelligence to improve colonoscopy adenoma detection rates.'),
            ('NCT018', 'This study uses reinforcement learning to optimize dosing and treatment allocation in an adaptive platform.'),
            ('NCT019', 'We apply deep learning and neural network models to predict sepsis onset from ICU monitoring data.')
        ) AS t(nct_id, description)
    """)

    conn.execute("""
        CREATE TABLE raw.keywords AS
        SELECT * FROM (VALUES
            ('NCT001', 'adaptive design'),
            ('NCT002', 'basket trial'),
            ('NCT004', 'platform trial'),
            ('NCT005', 'randomized controlled trial'),
            ('NCT007', 'SMART'),
            ('NCT008', 'pragmatic trial'),
            ('NCT012', 'bayesian'),
            ('NCT015', 'digital twin'),
            ('NCT017', 'artificial intelligence'),
            ('NCT019', 'machine learning')
        ) AS t(nct_id, name)
    """)

    return conn


def test_adaptive_detected_in_title():
    """'adaptive' in brief_title should trigger a match."""
    conn = _setup_features_test_db()
    detect_innovative_features(conn)

    result = conn.execute("""
        SELECT feature_type, source_field FROM class.innovative_features
        WHERE nct_id = 'NCT001' AND feature_type = 'adaptive' AND source_field = 'brief_title'
    """).fetchone()
    assert result is not None
    conn.close()


def test_adaptive_excluded_for_behavior():
    """'adaptive behavior' should NOT trigger an adaptive match."""
    conn = _setup_features_test_db()
    detect_innovative_features(conn)

    result = conn.execute("""
        SELECT * FROM class.innovative_features
        WHERE nct_id = 'NCT006' AND feature_type = 'adaptive'
    """).fetchall()
    assert len(result) == 0
    conn.close()


def test_basket_detected():
    """'basket' in title should be detected."""
    conn = _setup_features_test_db()
    detect_innovative_features(conn)

    result = conn.execute("""
        SELECT * FROM class.innovative_features
        WHERE nct_id = 'NCT002' AND feature_type = 'basket'
    """).fetchall()
    assert len(result) > 0
    conn.close()


def test_platform_requires_trial_context():
    """Bare 'platform' (NCT011) should NOT match; 'platform trial' (NCT004) should."""
    conn = _setup_features_test_db()
    detect_innovative_features(conn)

    # NCT004 has "Platform Trial" — should match
    result_004 = conn.execute("""
        SELECT * FROM class.innovative_features
        WHERE nct_id = 'NCT004' AND feature_type = 'platform'
    """).fetchall()
    assert len(result_004) > 0

    # NCT011 has "Digital Health Platform" — should NOT match
    result_011 = conn.execute("""
        SELECT * FROM class.innovative_features
        WHERE nct_id = 'NCT011' AND feature_type = 'platform'
    """).fetchall()
    assert len(result_011) == 0
    conn.close()


def test_smart_case_sensitive():
    """'SMART' (uppercase) should match; lowercase 'smart' should not."""
    conn = _setup_features_test_db()
    detect_innovative_features(conn)

    # NCT007 has "SMART" in title and keywords
    result = conn.execute("""
        SELECT * FROM class.innovative_features
        WHERE nct_id = 'NCT007' AND feature_type = 'SMART'
    """).fetchall()
    assert len(result) > 0
    conn.close()


def test_smart_full_phrase_detected():
    """'sequential multiple assignment randomized' should also trigger SMART."""
    conn = _setup_features_test_db()
    detect_innovative_features(conn)

    # NCT007 official_title has the full phrase
    result = conn.execute("""
        SELECT * FROM class.innovative_features
        WHERE nct_id = 'NCT007' AND feature_type = 'SMART'
        AND source_field = 'official_title'
    """).fetchall()
    assert len(result) > 0
    conn.close()


def test_multiple_features_same_study():
    """A study can have multiple innovative features detected."""
    conn = _setup_features_test_db()
    detect_innovative_features(conn)

    # NCT012 has both "seamless" and "bayesian" (in description)
    features = conn.execute("""
        SELECT DISTINCT feature_type FROM class.innovative_features
        WHERE nct_id = 'NCT012'
    """).fetchall()
    feature_types = {r[0] for r in features}
    assert "seamless" in feature_types
    assert "bayesian" in feature_types
    conn.close()


def test_enrichment_requires_design_context():
    """Bare 'enrichment' (NCT009) should NOT match; 'enrichment design' (NCT010) should."""
    conn = _setup_features_test_db()
    detect_innovative_features(conn)

    # NCT009: "Nutritional Enrichment" — should NOT match
    result_009 = conn.execute("""
        SELECT * FROM class.innovative_features
        WHERE nct_id = 'NCT009' AND feature_type = 'enrichment'
    """).fetchall()
    assert len(result_009) == 0

    # NCT010: "Enrichment Design" — should match
    result_010 = conn.execute("""
        SELECT * FROM class.innovative_features
        WHERE nct_id = 'NCT010' AND feature_type = 'enrichment'
    """).fetchall()
    assert len(result_010) > 0
    conn.close()


def test_matched_text_captured():
    """matched_text should contain the text that triggered the match."""
    conn = _setup_features_test_db()
    detect_innovative_features(conn)

    result = conn.execute("""
        SELECT matched_text FROM class.innovative_features
        WHERE nct_id = 'NCT002' AND feature_type = 'basket'
        AND source_field = 'brief_title'
    """).fetchone()
    assert result is not None
    assert "basket" in result[0].lower()
    conn.close()


def test_no_features_for_standard_trial():
    """A standard trial (NCT005) should not have any innovative features."""
    conn = _setup_features_test_db()
    detect_innovative_features(conn)

    result = conn.execute("""
        SELECT * FROM class.innovative_features
        WHERE nct_id = 'NCT005'
    """).fetchall()
    assert len(result) == 0
    conn.close()


def test_master_protocol_detected():
    """'master protocol' should be detected."""
    conn = _setup_features_test_db()
    detect_innovative_features(conn)

    result = conn.execute("""
        SELECT * FROM class.innovative_features
        WHERE nct_id = 'NCT013' AND feature_type = 'master protocol'
    """).fetchall()
    assert len(result) > 0
    conn.close()


def test_n_of_1_detected():
    """'N-of-1' should be detected."""
    conn = _setup_features_test_db()
    detect_innovative_features(conn)

    result = conn.execute("""
        SELECT * FROM class.innovative_features
        WHERE nct_id = 'NCT014' AND feature_type = 'N-of-1'
    """).fetchall()
    assert len(result) > 0
    conn.close()


def test_pragmatic_detected():
    """'pragmatic' should be detected."""
    conn = _setup_features_test_db()
    detect_innovative_features(conn)

    result = conn.execute("""
        SELECT * FROM class.innovative_features
        WHERE nct_id = 'NCT008' AND feature_type = 'pragmatic'
    """).fetchall()
    assert len(result) > 0
    conn.close()


# --- AI-augmented design pattern tests ---


def test_digital_twin_detected():
    """'digital twin' should trigger the digital twin feature."""
    conn = _setup_features_test_db()
    detect_innovative_features(conn)

    result = conn.execute("""
        SELECT * FROM class.innovative_features
        WHERE nct_id = 'NCT015' AND feature_type = 'digital twin'
    """).fetchall()
    assert len(result) > 0
    conn.close()


def test_in_silico_trial_detected():
    """'in silico trial' should trigger the in silico feature."""
    conn = _setup_features_test_db()
    detect_innovative_features(conn)

    result = conn.execute("""
        SELECT * FROM class.innovative_features
        WHERE nct_id = 'NCT016' AND feature_type = 'in silico'
    """).fetchall()
    assert len(result) > 0
    conn.close()


def test_ai_augmented_design_detected():
    """'AI-Guided Trial Design' in title should trigger AI-augmented design."""
    conn = _setup_features_test_db()
    detect_innovative_features(conn)

    result = conn.execute("""
        SELECT * FROM class.innovative_features
        WHERE nct_id = 'NCT018' AND feature_type = 'AI-augmented design'
    """).fetchall()
    assert len(result) > 0
    conn.close()


def test_ai_intervention_not_flagged_as_design():
    """AI as intervention (NCT017 'AI-Driven Screening') should NOT be AI-augmented design."""
    conn = _setup_features_test_db()
    detect_innovative_features(conn)

    result = conn.execute("""
        SELECT * FROM class.innovative_features
        WHERE nct_id = 'NCT017' AND feature_type = 'AI-augmented design'
    """).fetchall()
    assert len(result) == 0
    conn.close()


def test_reinforcement_learning_dosing_detected():
    """RL + dosing context in description should trigger AI-augmented design."""
    conn = _setup_features_test_db()
    detect_innovative_features(conn)

    result = conn.execute("""
        SELECT * FROM class.innovative_features
        WHERE nct_id = 'NCT018' AND feature_type = 'AI-augmented design'
        AND source_field = 'description'
    """).fetchall()
    assert len(result) > 0
    conn.close()


# --- AI-mention flag tests ---


def test_ai_mentions_detects_artificial_intelligence():
    """Studies mentioning 'artificial intelligence' should appear in ai_mentions."""
    conn = _setup_features_test_db()
    detect_ai_mentions(conn)

    result = conn.execute("""
        SELECT * FROM class.ai_mentions
        WHERE nct_id = 'NCT017' AND ai_term = 'artificial intelligence'
    """).fetchall()
    assert len(result) > 0
    conn.close()


def test_ai_mentions_detects_machine_learning():
    """Studies mentioning 'machine learning' should appear in ai_mentions."""
    conn = _setup_features_test_db()
    detect_ai_mentions(conn)

    result = conn.execute("""
        SELECT * FROM class.ai_mentions
        WHERE nct_id = 'NCT019' AND ai_term = 'machine learning'
    """).fetchall()
    assert len(result) > 0
    conn.close()


def test_ai_mentions_detects_deep_learning():
    """'deep learning' in description should be flagged."""
    conn = _setup_features_test_db()
    detect_ai_mentions(conn)

    result = conn.execute("""
        SELECT * FROM class.ai_mentions
        WHERE nct_id = 'NCT019' AND ai_term = 'deep learning'
    """).fetchall()
    assert len(result) > 0
    conn.close()


def test_ai_mentions_detects_neural_network():
    """'neural network' in description should be flagged."""
    conn = _setup_features_test_db()
    detect_ai_mentions(conn)

    result = conn.execute("""
        SELECT * FROM class.ai_mentions
        WHERE nct_id = 'NCT019' AND ai_term = 'neural network'
    """).fetchall()
    assert len(result) > 0
    conn.close()


def test_ai_mentions_no_false_positives():
    """A standard drug study (NCT020) should not appear in ai_mentions."""
    conn = _setup_features_test_db()
    detect_ai_mentions(conn)

    result = conn.execute("""
        SELECT * FROM class.ai_mentions
        WHERE nct_id = 'NCT020'
    """).fetchall()
    assert len(result) == 0
    conn.close()


def test_ai_mentions_keyword_source():
    """AI terms in keywords should be picked up."""
    conn = _setup_features_test_db()
    detect_ai_mentions(conn)

    result = conn.execute("""
        SELECT * FROM class.ai_mentions
        WHERE nct_id = 'NCT017' AND source_field = 'keyword'
        AND ai_term = 'artificial intelligence'
    """).fetchall()
    assert len(result) > 0
    conn.close()
