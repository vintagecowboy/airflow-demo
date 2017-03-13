SELECT
  rslt.*,
  area.lat,
  area.long
FROM
  [google.com:rich-lab:airflow_referendum.result_data] AS rslt
JOIN
  [google.com:rich-lab:airflow_referendum.admin_areas] AS area
ON
  rslt.area_code = area.gss
LIMIT
  1000
