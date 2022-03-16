
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

with source_data as (

    select 1 as id, current_timestamp() as dt
    union all
    select null as id, current_timestamp() as dt

)

select *
from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

where id is not null

/*

mis-use ref to test the effectivity of the source freshness command
{{ source("freshness_tests", "my_first_dbt_model") }}
{{ source("freshness_tests", "my_second_dbt_model") }}

*/
