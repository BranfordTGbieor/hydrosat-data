select
  batch_id,
  partition_date,
  tile_id,
  count(*) as observation_count,
  round(avg(surface_temp_c), 2) as avg_surface_temp_c,
  max(ndvi) as max_ndvi,
  sum(case when quality_band = 'high' then 1 else 0 end) as high_quality_count,
  sum(case when quality_band = 'moderate' then 1 else 0 end) as moderate_quality_count
from {{ ref('stg_satellite_observations') }}
group by 1, 2, 3
order by tile_id
