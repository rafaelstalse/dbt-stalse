{{ config(materialized='table') }}

WITH parsed_json_contacts AS (
    SELECT 
        JSON_EXTRACT_SCALAR(_airbyte_data, "$.properties.created_at") AS created_at,
        JSON_EXTRACT_SCALAR(_airbyte_data, "$.properties.createdate") AS createdate,
        JSON_EXTRACT_SCALAR(_airbyte_data, "$.properties.hs_latest_source_data_1") AS hs_latest_source_data_1,
        JSON_EXTRACT_SCALAR(_airbyte_data, "$.properties.associatedcompanyid") AS associatedcompanyid,
        JSON_EXTRACT_SCALAR(_airbyte_data, "$.properties.hs_analytics_source") AS hs_analytics_source,
        JSON_EXTRACT_SCALAR(_airbyte_data, "$.properties.id_adm") AS id_adm,
        JSON_EXTRACT_SCALAR(_airbyte_data, "$.properties.hs_latest_source") AS hs_latest_source,
        JSON_EXTRACT_SCALAR(_airbyte_data, "$.properties.id_fretebras__automacao") AS id_fretebras__automacao,
        JSON_EXTRACT_SCALAR(_airbyte_data, "$.properties.hs_email_last_email_name") AS hs_email_last_email_name,
        JSON_EXTRACT_SCALAR(_airbyte_data, "$.properties.num_conversion_events") AS num_conversion_events,
        JSON_EXTRACT_SCALAR(_airbyte_data, "$.properties.hs_analytics_source_data_2") AS hs_analytics_source_data_2,
        JSON_EXTRACT_SCALAR(_airbyte_data, "$.properties.lifecyclestage") AS lifecyclestage,
        JSON_EXTRACT_SCALAR(_airbyte_data, "$.properties.hs_analytics_last_url") AS hs_analytics_last_url,
        JSON_EXTRACT_SCALAR(_airbyte_data, "$.properties.recent_conversion_event_name") AS recent_conversion_event_name,
        JSON_EXTRACT_SCALAR(_airbyte_data, "$.properties.hs_analytics_first_url") AS hs_analytics_first_url,
        JSON_EXTRACT_SCALAR(_airbyte_data, "$.properties.hs_analytics_last_referrer") AS hs_analytics_last_referrer,
        JSON_EXTRACT_SCALAR(_airbyte_data, "$.properties.hs_analytics_first_referrer") AS hs_analytics_first_referrer,
        JSON_EXTRACT_SCALAR(_airbyte_data, "$.properties.cf_status") AS cf_status,
        JSON_EXTRACT_SCALAR(_airbyte_data, "$.properties.first_activation_date") AS first_activation_date,
        JSON_EXTRACT_SCALAR(_airbyte_data, "$.properties.last_activation_date") AS last_activation_date,
        JSON_EXTRACT_SCALAR(_airbyte_data, "$.properties.conversion_identifier") AS conversion_identifier,
        JSON_EXTRACT_SCALAR(_airbyte_data, "$.properties.cf_identificador_lead") AS cf_identificador_lead,
        JSON_EXTRACT_SCALAR(_airbyte_data, "$.properties.cnpj__sim_nao") AS cnpj__sim_nao,
        JSON_EXTRACT_SCALAR(_airbyte_data, "$.properties.ramo") AS ramo,
        JSON_EXTRACT_SCALAR(_airbyte_data, "$.properties.utm_source") AS utm_source,
        JSON_EXTRACT_SCALAR(_airbyte_data, "$.properties.blip__ticket_id") AS blip__ticket_id,
        JSON_EXTRACT_SCALAR(_airbyte_data, "$.properties.origem_blip") AS origem_blip,
        JSON_EXTRACT_SCALAR(_airbyte_data, "$.properties.blip__ticket_id_interno") AS blip__ticket_id_interno,
        JSON_EXTRACT_SCALAR(_airbyte_data, "$.properties.hs_analytics_first_touch_converting_campaign") AS hs_analytics_first_touch_converting_campaign,
        JSON_EXTRACT_SCALAR(_airbyte_data, "$.properties.hs_analytics_last_touch_converting_campaign") AS hs_analytics_last_touch_converting_campaign
    FROM {{ ref('base_contacts') }}
)

SELECT * FROM parsed_json_contacts
LIMIT 1000