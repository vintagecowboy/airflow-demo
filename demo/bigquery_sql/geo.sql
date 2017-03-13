SELECT
  rslt.*,
  area.lat,
  area.long
FROM
  [some-project:airflow_referendum.result_data] AS rslt
JOIN
  [some-project:airflow_referendum.admin_areas] AS area
ON
  rslt.area_code = area.gss
LIMIT
  1000
