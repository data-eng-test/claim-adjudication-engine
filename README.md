# claim-adjudication-engine

Adjudicates staged insurance claims using coverage rules, medical necessity
logic, and benefit limits. Produces a final decision per claim.

## Decisions
- APPROVED — claim passes all rules, full payment authorised
- DENIED   — claim fails coverage or eligibility checks
- PARTIAL  — claim partially covered (e.g. out-of-network rate applied)
- PENDED   — requires manual review (complex diagnosis, high value > $50k)

## Tech Stack
- Orchestration: AWS Step Functions + Airflow trigger
- Processing: AWS Glue PySpark (rules engine)
- Rules storage: Redshift `claims_rules.dim_coverage_rules`
- Output: Redshift `claims_adjudicated.fact_claim_adjudication`
- Monitoring: CloudWatch + PagerDuty

## SLA
- Claims adjudicated within 4 hours of landing in staging
- PENDED claims: manual review within 24 business hours

## Dependencies
- Upstream: claim-intake-pipeline (claims_staging.stg_claim_raw)
- Downstream: claim-payments-reconciliation reads APPROVED/PARTIAL decisions

## Key Contacts
- Rules Owner: Dr. Amit Patel (amit.patel@insurer.com) — Clinical Rules
- Engineering: Nadia Torres (nadia.torres@insurer.com)
- On-call: #claims-adjudication-alerts
