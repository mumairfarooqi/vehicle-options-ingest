 SELECT

  trim(REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(raw_tbl.content, "$.Model"), ".+")) as model,
  trim(REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(raw_tbl.content, "$.Option_Code"), ".+")) as option_code,
  trim(REGEXP_REPLACE(JSON_EXTRACT_SCALAR(raw_tbl.content, "$.Option_Desc"),"\n", " ")) as option_desc,
  CAST(REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(raw_tbl.content, "$.Material_Cost"), ".+") AS NUMERIC) as material_cost

FROM
  `breuninger-datalake-dev.airflow_playground.options_data_raw` raw_tbl
 ;