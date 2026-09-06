-- What this proves: the neutral type each declaration states reaches the
-- built relation on the reference adapter, for the fact and for a dimension.
--
-- A declaration says decimal with a precision and a scale, integer, date or
-- string, and the adapter's own conventions say what each of those is on the
-- platform. Nothing between the two is visible in a row count or a sum: a
-- revenue column built as a floating point number would still add up here,
-- and would stop adding up somewhere else later. So the built types are read
-- from the adapter's own catalogue and compared with what the adapter says
-- each declared type is.
--
-- Singular test: it passes when it returns no rows.
with built as (
    select
        table_name,
        column_name,
        upper(data_type) as data_type
    from information_schema.columns
    where table_name in (
        '{{ ref('ecommerce__order_star__fact_order_line').identifier }}',
        '{{ ref('ecommerce__order_star__dim_product').identifier }}'
    )
),

expected (table_name, column_name, data_type) as (
    values
        ('{{ ref('ecommerce__order_star__fact_order_line').identifier }}', 'order_id', 'VARCHAR'),
        ('{{ ref('ecommerce__order_star__fact_order_line').identifier }}', 'line_number', 'INTEGER'),
        ('{{ ref('ecommerce__order_star__fact_order_line').identifier }}', 'line_quantity', 'INTEGER'),
        ('{{ ref('ecommerce__order_star__fact_order_line').identifier }}', 'line_revenue', 'DECIMAL(12,2)'),
        ('{{ ref('ecommerce__order_star__fact_order_line').identifier }}', 'line_list_value', 'DECIMAL(12,2)'),
        ('{{ ref('ecommerce__order_star__fact_order_line').identifier }}', 'line_order_date', 'DATE'),
        ('{{ ref('ecommerce__order_star__dim_product').identifier }}', 'product_code', 'VARCHAR'),
        ('{{ ref('ecommerce__order_star__dim_product').identifier }}', 'product_list_price', 'DECIMAL(12,2)')
)

select
    expected.table_name,
    expected.column_name,
    'the built type is not the declared type' as disagreement
from expected
left join built
    on built.table_name = expected.table_name
   and built.column_name = expected.column_name
where built.column_name is null
   or built.data_type <> expected.data_type
