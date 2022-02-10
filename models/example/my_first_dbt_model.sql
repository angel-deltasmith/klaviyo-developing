
SELECT
*
FROM raw_national_review._airbyte_raw_widget_desktop

SELECT
    {{ json_extract_scalar('_airbyte_data', ['date'])}} as date
    , {{ json_extract_scalar('_airbyte_data', ['widget_imps'])}}  as impressions
    , {{ json_extract_scalar('_airbyte_data', ['ad_clicks'])}}  as clicks
    , {{ json_extract_scalar('_airbyte_data', ['ad_revenue'])}}  as revenue
FROM raw_national_review._airbyte_raw_widget_desktop