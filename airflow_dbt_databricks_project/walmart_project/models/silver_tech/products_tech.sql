{{
    config
    (
        materialized='incremental',
        unique_key='product_id'
    )
}}

SELECT *,
    CURRENT_TIMESTAMP() AS processed_at
FROM 
    {{ source('walmart_databricks', 'products') }}

{% if is_incremental() %}
    WHERE updated_timestamp > (SELECT COALESCE(MAX(updated_timestamp), '1900-01-01') FROM {{ this }})
{% endif %}