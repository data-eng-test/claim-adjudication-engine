"""
medical_necessity.py
AWS Lambda function — evaluates medical necessity of a claim.
Called as Step Functions state: medical-necessity-scorer.
Returns: APPROVED, DENIED, or PENDED with reason codes.
"""
import json
import boto3
import logging
from typing import Tuple

logger = logging.getLogger(__name__)

# Procedure codes requiring prior authorisation
PRIOR_AUTH_REQUIRED_CODES = {
    "27447",  # Total knee replacement
    "27130",  # Total hip replacement
    "43239",  # Upper GI endoscopy
    "70553",  # MRI brain with contrast
    "74177",  # CT abdomen/pelvis with contrast
    "93306",  # Echocardiography
}

# Experimental/investigational codes — auto-deny
EXPERIMENTAL_CODES = {
    "0101T", "0102T", "0103T",  # Reserved experimental category I
    "S9090",  # Vertebral axial decompression
}

# High-value threshold for PEND (manual review)
HIGH_VALUE_THRESHOLD_USD = 50_000.00

sagemaker_runtime = boto3.client("sagemaker-runtime", region_name="us-east-1")
FRAUD_MODEL_ENDPOINT = "claims-fraud-model-v3"


def evaluate_medical_necessity(event: dict, context) -> dict:
    """
    Lambda handler — evaluates medical necessity for a single claim.
    Input event: { claim_id, claim_type, procedure_codes, diagnosis_codes,
                   billed_amount, member_id, prior_auth_obtained }
    Returns: { decision, reason_code, reason_description, fraud_score }
    """
    claim_id        = event["claim_id"]
    procedure_codes = set(event.get("procedure_codes", "").split("|"))
    billed_amount   = float(event.get("billed_amount", 0))
    prior_auth      = event.get("prior_auth_obtained", False)

    # Check experimental procedures — auto-deny
    experimental = procedure_codes & EXPERIMENTAL_CODES
    if experimental:
        return _build_response("DENIED", "EXP001",
            f"Experimental procedure code(s): {', '.join(experimental)}", 0.0)

    # Check prior auth required
    auth_required = procedure_codes & PRIOR_AUTH_REQUIRED_CODES
    if auth_required and not prior_auth:
        return _build_response("DENIED", "AUTH001",
            f"Prior authorisation required for: {', '.join(auth_required)}", 0.0)

    # High value — pend for manual review
    if billed_amount > HIGH_VALUE_THRESHOLD_USD:
        return _build_response("PENDED", "HIGH_VALUE",
            f"Billed amount ${billed_amount:,.2f} exceeds ${HIGH_VALUE_THRESHOLD_USD:,.2f} manual review threshold", 0.0)

    # Fraud scoring via SageMaker
    # Known issue CAE-007: cold start causes 45s delay on first call per hour
    fraud_score = _get_fraud_score(event)
    if fraud_score > 0.85:
        return _build_response("PENDED", "FRAUD_RISK",
            f"Fraud risk score {fraud_score:.2f} exceeds threshold 0.85", fraud_score)

    return _build_response("APPROVED", "MED_NEC_PASS",
        "Claim passes medical necessity evaluation", fraud_score)


def _get_fraud_score(claim_data: dict) -> float:
    """Invoke SageMaker fraud detection endpoint."""
    try:
        response = sagemaker_runtime.invoke_endpoint(
            EndpointName=FRAUD_MODEL_ENDPOINT,
            ContentType="application/json",
            Body=json.dumps({
                "provider_npi":    claim_data.get("provider_npi"),
                "claim_type":      claim_data.get("claim_type"),
                "billed_amount":   claim_data.get("billed_amount"),
                "procedure_codes": claim_data.get("procedure_codes"),
                "diagnosis_codes": claim_data.get("diagnosis_codes"),
                "member_id":       claim_data.get("member_id"),
            }),
        )
        result = json.loads(response["Body"].read())
        return float(result.get("fraud_probability", 0.0))
    except Exception as e:
        logger.error(f"SageMaker fraud scoring failed for {claim_data.get('claim_id')}: {e}")
        return 0.0  # Default to no fraud risk on scoring failure


def _build_response(decision: str, code: str, description: str, fraud_score: float) -> dict:
    return {
        "decision":           decision,
        "reason_code":        code,
        "reason_description": description,
        "fraud_score":        fraud_score,
    }
