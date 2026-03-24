"""
coverage_rules.py
Loads and applies coverage rules from Redshift dim_coverage_rules table.
Rules are cached in memory with 24h TTL.
Known issue CAE-003: 24h TTL means rule updates take up to 24h to propagate.
Fix pending: cache invalidation webhook (CAE-003).
"""
import boto3
import psycopg2
import json
import logging
from datetime import datetime, timedelta
from decimal import Decimal
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)

_rules_cache = {}
_cache_loaded_at: Optional[datetime] = None
CACHE_TTL_HOURS = 24  # CAE-003: reduce this with invalidation webhook


@dataclass
class CoverageRule:
    policy_number:          str
    claim_type:             str
    plan_type:              str
    deductible_annual_usd:  Decimal
    oop_max_usd:            Decimal
    in_network_rate:        Decimal
    out_of_network_rate:    Decimal
    prior_auth_required:    bool
    excluded_cpt_codes:     list
    benefit_limit_usd:      Optional[Decimal]
    rule_version:           str


def load_coverage_rules() -> dict:
    """Load all active coverage rules from Redshift into memory cache."""
    global _rules_cache, _cache_loaded_at

    if _cache_loaded_at and (datetime.utcnow() - _cache_loaded_at) < timedelta(hours=CACHE_TTL_HOURS):
        logger.debug("Using cached coverage rules")
        return _rules_cache

    logger.info("Loading coverage rules from Redshift...")
    secret = _get_secret("claims/redshift/adjudication")
    conn = psycopg2.connect(
        host="claims-prod-cluster.us-east-1.redshift.amazonaws.com",
        port=5439, dbname="claimsdb",
        user="claims_adjudication",
        password=secret["password"],
        sslmode="require",
    )
    cur = conn.cursor()
    cur.execute("""
        SELECT
            policy_number, claim_type, plan_type,
            deductible_annual_usd, oop_max_usd,
            in_network_rate, out_of_network_rate,
            prior_auth_required, excluded_cpt_codes,
            benefit_limit_usd, rule_version
        FROM claims_rules.dim_coverage_rules
        WHERE is_active = TRUE
        AND effective_date <= CURRENT_DATE
        AND (expiry_date IS NULL OR expiry_date > CURRENT_DATE)
        ORDER BY effective_date DESC
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    _rules_cache = {}
    for row in rows:
        key = f"{row[0]}|{row[1]}"  # policy_number|claim_type
        if key not in _rules_cache:  # first row = most recent rule
            _rules_cache[key] = CoverageRule(
                policy_number=row[0], claim_type=row[1], plan_type=row[2],
                deductible_annual_usd=row[3], oop_max_usd=row[4],
                in_network_rate=row[5], out_of_network_rate=row[6],
                prior_auth_required=bool(row[7]),
                excluded_cpt_codes=row[8].split("|") if row[8] else [],
                benefit_limit_usd=row[9], rule_version=row[10],
            )

    _cache_loaded_at = datetime.utcnow()
    logger.info(f"Loaded {len(_rules_cache)} coverage rules (version cache: {_cache_loaded_at})")
    return _rules_cache


def get_rule_for_claim(policy_number: str, claim_type: str) -> Optional[CoverageRule]:
    """Return the coverage rule for a given policy and claim type."""
    rules = load_coverage_rules()
    return rules.get(f"{policy_number}|{claim_type}")


def check_excluded_procedure(rule: CoverageRule, procedure_codes: str) -> Optional[str]:
    """Return the excluded CPT code if any procedure is excluded."""
    codes = procedure_codes.split("|") if procedure_codes else []
    for code in codes:
        if code in rule.excluded_cpt_codes:
            return code
    return None


def calculate_approved_amount(
    rule: CoverageRule,
    billed_amount: Decimal,
    network_status: str,
    deductible_met: Decimal,
) -> Decimal:
    """
    Calculate the approved payment amount after applying coverage rules.
    Known issue CAE-005: DENTAL OON rate not correctly applied — fix in progress.
    """
    # Apply network rate
    if network_status == "OUT_OF_NETWORK":
        rate = rule.out_of_network_rate  # CAE-005: DENTAL uses wrong rate here
    else:
        rate = rule.in_network_rate

    covered_amount = billed_amount * rate

    # Apply remaining deductible
    remaining_deductible = max(Decimal("0"), rule.deductible_annual_usd - deductible_met)
    covered_amount = max(Decimal("0"), covered_amount - remaining_deductible)

    # Apply benefit limit if set
    if rule.benefit_limit_usd:
        covered_amount = min(covered_amount, rule.benefit_limit_usd)

    return covered_amount.quantize(Decimal("0.01"))


def _get_secret(secret_name: str) -> dict:
    client = boto3.client("secretsmanager", region_name="us-east-1")
    return json.loads(client.get_secret_value(SecretId=secret_name)["SecretString"])
