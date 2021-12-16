{{ config(materialized='incremental') }}

select
    CAST(id AS string),
    CAST(user_id AS string),
    CAST(wallet_tx_id AS string),
    CAST(amount AS integer),
    CAST("status" AS string),
    CAST("type" AS string),
    CAST(created_at AS timestamp),
    CAST(created_by AS string),
    CAST(updated_at AS timestamp),
    CAST(updated_at AS string)
from `data-314708.test_referral_v2.transactions`

{% if is_incremental() %}

    -- this filter will only be applied on an incremental run
    where updated_at > (select max(updated_at) from {{ this }})

{% endif %}