-- dim_coverage_rules.sql
-- Dimension table: all active coverage rules per policy + claim type combination
-- Source: Manual uploads by Clinical Rules team + automated policy system sync
-- Updated: Monthly by Dr. Amit Patel (Clinical Rules)
-- Known issue CAE-003: 24h cache TTL in coverage_rules.py delays rule propagation

{{ config(
    materialized = 'table',
    schema       = 'claims_rules'
) }}

SELECT
    policy_number,
    claim_type,                          -- MEDICAL, DENTAL, VISION, PHARMACY
    plan_type,                           -- PPO, HMO, EPO, HDHP
    deductible_annual_usd,
    oop_max_usd,                         -- Out-of-pocket maximum
    in_network_rate,                     -- e.g. 0.80 = 80% covered in-network
    out_of_network_rate,                 -- e.g. 0.60 = 60% covered OON
    prior_auth_required,
    excluded_cpt_codes,                  -- Pipe-delimited list of excluded CPT codes
    benefit_limit_usd,                   -- Annual benefit cap (NULL = no limit)
    effective_date,
    expiry_date,
    rule_version,
    is_active,
    last_updated_by,
    last_updated_at
FROM {{ source('claims_rules_raw', 'coverage_rules_raw') }}
WHERE is_active = TRUE
ORDER BY policy_number, claim_type, effective_date DESC
