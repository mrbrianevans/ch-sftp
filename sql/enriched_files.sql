
create or replace view enriched_files as
with base as (
  select
    *,
    regexp_match(path, '^/free/([^/]+)') as m1,
    regexp_match(path, '/(\d{4})/(\d{2})/(\d{2})/([^/]+)$') as m_date,
    regexp_match(path, '[^/]+$') as m_fname
  from files
)
select
  b.path,
  b.size_bytes,
  b.last_modified,
  b.crawled_at,
  b.ingested_at,
  case when (b.m1)[1] ~* '^prod' then (b.m1)[1] else null end as product_code,
  case
    when b.m_date is not null then 'data'
    when (b.m1)[1] ~* '^prod' then 'documentation'
    else 'other'
  end as file_category,
  (b.m_date)[1]::int as production_year,
  (b.m_date)[2]::int as production_month,
  (b.m_date)[3]::int as production_day,
  case
    when b.m_date is not null
         and (b.m_date)[1]::int between 2010 and 2035
         and (b.m_date)[2]::int between 1 and 12
         and (b.m_date)[3]::int between 1 and 31
    then make_date((b.m_date)[1]::int, (b.m_date)[2]::int, (b.m_date)[3]::int)
    else null
  end as production_date,
  (b.m_fname)[1] as filename,
  case
    when (b.m_fname)[1] ~ '\.'
    then lower((regexp_match((b.m_fname)[1], '\.([a-z0-9]+)$'))[1])
    else null
  end as file_extension,
  case
    when b.m_date is not null
    then (regexp_match((b.m_fname)[1], '(?:^|[_-])(\d{3,6})(?:[_\.-]|$)'))[1]::int
    else null
  end as run_number
from base b;