SELECT
    '{{ env_var("dbt_env_var_1") }}' AS env_var_1
  , '{{ env_var("dbt_env_var_2") }}' AS env_var_2

-- from {{ ref('my_second_dbt_model') }} fake ref
