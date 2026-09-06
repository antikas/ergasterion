# Reference-model mappings with no canonical product

The source declarations under `declarations/` once recorded, per source and per entity, which delivered column carries which reference-model attribute. That record now lives in the `reference` block of each canonical product under this directory, where `ergasterion validate-canonical` proves it against the reference model checkout.

Three entities and seven attributes in the old record have no canonical product to hold them, because no product in this estate publishes the entity or the column. They are kept here so the mapping knowledge survives until a product exists for it. Every line reads `reference attribute <- delivered column`.

## E-06 cash flow event (`model/entities/core/E-06-cash-flow-event.md`)

Business keys: `cash_flow_id` (event identifier), `instrument_id` (the fund or vehicle the event belongs to).

| Source | Business keys | Attributes |
|---|---|---|
| chrono | `cash_flow_id <- event_uid`, `instrument_id <- vehicle_code` | `cash_flow_date <- event_date`, `cash_flow_type <- event_kind`, `direction <- investor_direction`, `amount <- usd_amount`, `currency <- currency_code`, `source <- source_system` |
| meridex | `cash_flow_id <- cash_flow_id`, `instrument_id <- source_fund_id` | `cash_flow_date <- cash_flow_date`, `cash_flow_type <- cash_flow_type`, `direction <- direction`, `amount <- amount_usd`, `currency <- currency`, `source <- source_system` |
| portiq | `cash_flow_id <- cash_flow_id`, `instrument_id <- source_fund_id` | `cash_flow_date <- cash_flow_date`, `cash_flow_type <- cash_flow_type`, `direction <- direction`, `amount <- amount_usd`, `currency <- currency`, `source <- source_system` |
| vantora | `cash_flow_id <- cash_flow_id`, `instrument_id <- source_fund_id` | `cash_flow_date <- cash_flow_date`, `cash_flow_type <- cash_flow_type`, `direction <- direction`, `amount <- amount_usd`, `currency <- currency`, `source <- source_system` |

## E-07 valuation (`model/entities/core/E-07-valuation.md`)

Business keys: `valuation_id` (mark identifier), `instrument_id` (the fund or vehicle marked).

| Source | Business keys | Attributes |
|---|---|---|
| chrono | `valuation_id <- mark_uid`, `instrument_id <- vehicle_code` | `valuation_date <- mark_date`, `value_usd <- usd_fair_value`, `method <- mark_method`, `valuation_level <- fair_value_bucket`, `confidence_score <- extraction_confidence` |
| vantora | `valuation_id <- valuation_id`, `instrument_id <- source_fund_id` | `valuation_date <- valuation_date`, `value_usd <- value_usd`, `method <- method`, `valuation_level <- valuation_level`, `confidence_score <- confidence_score` |

## PM-11 manager succession event (`model/entities/specialisations/private-markets/PM-11-manager-succession-event.md`)

| Source | Business keys | Attributes |
|---|---|---|
| vantora | `succession_id <- succession_event_id`, `predecessor_gp_id <- predecessor_source_gp_id`, `successor_gp_id <- successor_source_gp_id` | `event_type <- event_type`, `effective_date <- effective_date` |

## Attributes with no counterpart column on their canonical product

| Reference entity | Attribute | Delivered columns that carried it |
|---|---|---|
| PM-01 fund and vehicle | `known_aliases` | chrono `vehicle_display_name`; meridex, portiq, vantora `fund_name` (as a business key) |
| PM-01 fund and vehicle | `last_reviewed_at` | chrono `snapshot_date`; vantora `as_of_date` |
| PM-01 fund and vehicle | `vehicle_type` | vantora `vehicle_type` |
| PM-02 general partner | `known_aliases` | chrono `manager_name_text`; meridex, portiq, vantora `gp_name` (as a business key) |
| PM-04 portfolio company | `known_aliases` | chrono `issuer_name_text`; meridex, portiq, vantora `company_name` (as a business key) |
| PM-04 portfolio company | `first_seen_at` | chrono `first_reported_on`; vantora `first_seen_at` |
| PM-15 deal | `sourced_date` | origo `sourced_date` |
