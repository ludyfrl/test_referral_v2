{{ config(materialized='incremental') }}

select
    CAST(id AS string) AS id,
    CAST(user_id AS string) AS user_id,
    CAST(wallet_tx_id AS string) AS wallet_tx_id,
    CAST(amount AS integer) AS amount,
    CAST(status AS string) AS status,
    CAST(type AS string) AS type,
    CAST(created_at AS timestamp) AS created_at,
    CAST(created_by AS string) AS created_by,
    CAST(updated_at AS timestamp) AS updated_at,
    CAST(updated_by AS string) AS updated_by
from `data-314708.test_referral_v2.transactions`

{% if is_incremental() %}

    -- this filter will only be applied on an incremental run
    where CAST(updated_at AS timestamp) > (select max(updated_at) from {{ this }})

{% endif %}