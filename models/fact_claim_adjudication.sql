-- fact_claim_adjudication.sql
-- Central fact table for all adjudicated claims
-- Joins staging claims with coverage rules and adjudication decisions

{{ config(
    materialized = 'incremental',
    unique_key   = 'claim_id',
    schema       = 'claims_adjudicated'
) }}

SELECT
    sc.claim_submission_id                              AS claim_id,
    sc.member_id,
    sc.policy_number,
    sc.provider_npi,
    sc.claim_type,
    sc.date_of_service,
    sc.total_billed_amount_usd,
    ad.adjudication_decision,                           -- APPROVED, DENIED, PARTIAL, PENDED
    ad.approved_amount_usd,
    ad.denial_reason_code,
    ad.denial_reason_description,
    ad.adjudicated_at,
    ad.adjudicated_by,                                  -- RULES_ENGINE or MANUAL_REVIEW
    cr.plan_type,
    cr.deductible_remaining_usd,
    cr.out_of_pocket_max_usd,
    sc.network_status,
    CASE
        WHEN sc.network_status = 'OUT_OF_NETWORK'
        THEN ad.approved_amount_usd * 0.60              -- 60% reimbursement OON
        ELSE ad.approved_amount_usd
    END AS final_payment_amount_usd
FROM {{ ref('stg_claim_raw') }} sc
LEFT JOIN {{ source('adjudication', 'adjudication_decisions') }} ad
    ON sc.claim_submission_id = ad.claim_id
LEFT JOIN {{ ref('dim_coverage_rules') }} cr
    ON sc.policy_number = cr.policy_number
    AND sc.claim_type = cr.claim_type

{% if is_incremental() %}
WHERE ad.adjudicated_at > (SELECT MAX(adjudicated_at) FROM {{ this }})
{% endif %}
