"""
claim_scorer.py
Orchestrates the full adjudication scoring pipeline for a single claim.
Called by the Step Functions Lambda write-adjudication-decision.
Combines coverage rules, medical necessity, and eligibility results into
a final adjudication decision written to Redshift.
"""
import boto3
import psycopg2
import json
import logging
from decimal import Decimal
from datetime import datetime
from src.rules.coverage_rules import get_rule_for_claim, calculate_approved_amount, check_excluded_procedure

logger = logging.getLogger(__name__)


DENIAL_CODES = {
    "NO_COVERAGE":   "Member not covered for this claim type on date of service",
    "EXCLUDED_CPT":  "Procedure code excluded from member benefits",
    "EXP001":        "Experimental or investigational procedure",
    "AUTH001":       "Prior authorisation not obtained",
    "ELIG_FAIL":     "Member eligibility verification failed",
    "BENEFIT_LIMIT": "Annual benefit limit exceeded",
}


def score_claim(claim: dict, eligibility_result: dict, necessity_result: dict) -> dict:
    """
    Produce final adjudication decision for a claim.
    Returns decision dict for writing to fact_claim_adjudication.
    """
    claim_id      = claim["claim_id"]
    policy_number = claim["policy_number"]
    claim_type    = claim["claim_type"]
    billed_amount = Decimal(str(claim["total_billed_amount_usd"]))
    network_status= claim["network_status"]

    # Eligibility check failed → DENIED
    if not eligibility_result.get("is_eligible", False):
        return _build_decision(claim_id, "DENIED", Decimal("0"),
            "ELIG_FAIL", DENIAL_CODES["ELIG_FAIL"], "RULES_ENGINE")

    # Medical necessity → DENIED or PENDED
    med_decision = necessity_result.get("decision", "APPROVED")
    if med_decision in ("DENIED", "PENDED"):
        return _build_decision(claim_id, med_decision, Decimal("0"),
            necessity_result["reason_code"],
            necessity_result["reason_description"], "RULES_ENGINE")

    # Load coverage rule
    rule = get_rule_for_claim(policy_number, claim_type)
    if not rule:
        return _build_decision(claim_id, "DENIED", Decimal("0"),
            "NO_COVERAGE", DENIAL_CODES["NO_COVERAGE"], "RULES_ENGINE")

    # Check excluded procedure codes
    excluded = check_excluded_procedure(rule, claim.get("procedure_codes", ""))
    if excluded:
        return _build_decision(claim_id, "DENIED", Decimal("0"),
            "EXCLUDED_CPT", f"{DENIAL_CODES['EXCLUDED_CPT']}: {excluded}", "RULES_ENGINE")

    # Calculate approved amount
    deductible_met = Decimal(str(eligibility_result.get("deductible_met_usd", "0")))
    approved = calculate_approved_amount(rule, billed_amount, network_status, deductible_met)

    if approved <= 0:
        return _build_decision(claim_id, "DENIED", Decimal("0"),
            "BENEFIT_LIMIT", DENIAL_CODES["BENEFIT_LIMIT"], "RULES_ENGINE")

    # Partial if approved < billed
    decision = "PARTIAL" if approved < billed_amount else "APPROVED"

    return _build_decision(claim_id, decision, approved, None, None, "RULES_ENGINE")


def _build_decision(claim_id, decision, approved_amount,
                    denial_code, denial_description, adjudicated_by):
    return {
        "claim_id":              claim_id,
        "adjudication_decision": decision,
        "approved_amount_usd":   float(approved_amount),
        "denial_reason_code":    denial_code,
        "denial_reason_description": denial_description,
        "adjudicated_at":        datetime.utcnow().isoformat(),
        "adjudicated_by":        adjudicated_by,
    }
