select
  batch_id,
  cast('{{ env_var("HYDROSAT_BATCH_DATE") }}' as date) as partition_date,
  scene_id,
  tile_id,
  cast(captured_at as timestamp) as captured_at,
  round(cast(surface_temp_c as double), 2) as surface_temp_c,
  round(cast(ndvi as double), 3) as ndvi,
  round(cast(cloud_cover_pct as double), 2) as cloud_cover_pct,
  case
    when cast(cloud_cover_pct as double) < 5 then 'high'
    else 'moderate'
  end as quality_band,
  case
    when cast(ndvi as double) >= 0.8 then 'dense'
    else 'medium'
  end as vegetation_band
from read_json_auto('{{ env_var("HYDROSAT_RAW_URI") }}', format='newline_delimited')
