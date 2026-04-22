{% macro export_lake_outputs() %}
  {% set staging_uri = env_var('SIGHT_POC_STAGING_URI') %}
  {% set curated_uri = env_var('SIGHT_POC_CURATED_URI') %}

  {% set export_staging %}
    copy (
      select * from {{ ref('stg_satellite_observations') }}
      order by tile_id, scene_id
    ) to '{{ staging_uri }}' (format json, array false);
  {% endset %}

  {% set export_curated %}
    copy (
      select
        batch_id,
        partition_date,
        tile_id,
        observation_count,
        avg_surface_temp_c,
        max_ndvi,
        json_object('high', high_quality_count, 'moderate', moderate_quality_count) as quality_band_breakdown
      from {{ ref('cur_satellite_tile_summary') }}
      order by tile_id
    ) to '{{ curated_uri }}' (format json, array true);
  {% endset %}

  {% do run_query(export_staging) %}
  {% do run_query(export_curated) %}

  {{ return({'staging_uri': staging_uri, 'curated_uri': curated_uri}) }}
{% endmacro %}
