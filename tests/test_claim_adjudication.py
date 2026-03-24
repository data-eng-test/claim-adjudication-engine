"""
test_claim_adjudication.py
Unit tests for adjudication scoring and coverage rules.
Run: pytest tests/ -v
"""
import pytest
from decimal import Decimal
from unittest.mock import patch, MagicMock
from src.rules.coverage_rules import CoverageRule, calculate_approved_amount, check_excluded_procedure
from src.scorer.claim_scorer import score_claim

SAMPLE_RULE = CoverageRule(
    policy_number="POL-2024-56789",
    claim_type="MEDICAL",
    plan_type="PPO",
    deductible_annual_usd=Decimal("1500.00"),
    oop_max_usd=Decimal("5000.00"),
    in_network_rate=Decimal("0.80"),
    out_of_network_rate=Decimal("0.60"),
    prior_auth_required=False,
    excluded_cpt_codes=["99499", "S9090"],
    benefit_limit_usd=None,
    rule_version="v2026.03",
)

SAMPLE_CLAIM = {
    "claim_id":              "CLM-2026-001234",
    "policy_number":         "POL-2024-56789",
    "claim_type":            "MEDICAL",
    "total_billed_amount_usd": 1000.00,
    "network_status":        "IN_NETWORK",
    "procedure_codes":       "99213|87880",
    "diagnosis_codes":       "J06.9",
}

SAMPLE_ELIGIBILITY = {"is_eligible": True, "deductible_met_usd": 500.00}
SAMPLE_NECESSITY   = {"decision": "APPROVED", "reason_code": "MED_NEC_PASS"}


class TestCoverageRules:

    def test_in_network_approved_amount(self):
        """In-network claim: 80% rate applied after deductible."""
        result = calculate_approved_amount(
            SAMPLE_RULE, Decimal("1000.00"), "IN_NETWORK", Decimal("1500.00")
        )
        assert result == Decimal("800.00")

    def test_out_of_network_rate(self):
        """Out-of-network claim: 60% rate applied."""
        result = calculate_approved_amount(
            SAMPLE_RULE, Decimal("1000.00"), "OUT_OF_NETWORK", Decimal("1500.00")
        )
        assert result == Decimal("600.00")

    def test_deductible_not_met_reduces_payment(self):
        """If deductible not fully met, remaining deductible reduces approved amount."""
        result = calculate_approved_amount(
            SAMPLE_RULE, Decimal("1000.00"), "IN_NETWORK", Decimal("0.00")
        )
        # 80% of 1000 = 800, minus 1500 deductible remaining = 0
        assert result == Decimal("0.00")

    def test_excluded_cpt_detected(self):
        """Excluded CPT code should be caught."""
        excluded = check_excluded_procedure(SAMPLE_RULE, "99213|99499|87880")
        assert excluded == "99499"

    def test_no_excluded_cpt(self):
        """No excluded codes should return None."""
        excluded = check_excluded_procedure(SAMPLE_RULE, "99213|87880")
        assert excluded is None


class TestClaimScorer:

    @patch("src.scorer.claim_scorer.get_rule_for_claim")
    def test_approved_decision(self, mock_rule):
        """Eligible claim with passing necessity should be APPROVED."""
        mock_rule.return_value = SAMPLE_RULE
        result = score_claim(SAMPLE_CLAIM, SAMPLE_ELIGIBILITY, SAMPLE_NECESSITY)
        assert result["adjudication_decision"] == "APPROVED"
        assert result["approved_amount_usd"] > 0

    def test_ineligible_claim_denied(self):
        """Ineligible member should receive DENIED decision."""
        ineligible = {"is_eligible": False}
        result = score_claim(SAMPLE_CLAIM, ineligible, SAMPLE_NECESSITY)
        assert result["adjudication_decision"] == "DENIED"
        assert result["denial_reason_code"] == "ELIG_FAIL"

    def test_pended_necessity_decision(self):
        """Claim pended by medical necessity should be PENDED."""
        pended_necessity = {"decision": "PENDED", "reason_code": "HIGH_VALUE",
                            "reason_description": "Amount exceeds threshold"}
        result = score_claim(SAMPLE_CLAIM, SAMPLE_ELIGIBILITY, pended_necessity)
        assert result["adjudication_decision"] == "PENDED"

    @patch("src.scorer.claim_scorer.get_rule_for_claim")
    def test_partial_decision_when_approved_less_than_billed(self, mock_rule):
        """Claim where approved < billed should be PARTIAL."""
        partial_rule = CoverageRule(
            **{**SAMPLE_RULE.__dict__,
               "in_network_rate": Decimal("0.60"),
               "benefit_limit_usd": Decimal("500.00")}
        )
        mock_rule.return_value = partial_rule
        claim = {**SAMPLE_CLAIM, "total_billed_amount_usd": 2000.00}
        result = score_claim(claim, SAMPLE_ELIGIBILITY, SAMPLE_NECESSITY)
        assert result["adjudication_decision"] == "PARTIAL"
