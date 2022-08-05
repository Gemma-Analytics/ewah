SELECT '{{ env_var("dbt_env_var_1") }}' AS env_var
-- from {{ ref('my_second_dbt_model') }} fake ref
