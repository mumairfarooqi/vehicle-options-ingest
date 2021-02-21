 SELECT

  trim(REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(raw_tbl.content, "$.nameplate_code"), ".+")) as nameplate_code,
  trim(REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(raw_tbl.content, "$.brand"), ".+")) as brand,
  trim(REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(raw_tbl.content, "$.platform"), ".+")) as platform,
  trim(REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(raw_tbl.content, "$.nameplate_display"), ".+")) as nameplate_display

FROM
  `breuninger-datalake-dev.airflow_playground.vehicle_line_mapping_raw` raw_tbl

  where REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(raw_tbl.content, "$.nameplate_code"), ".+") is not null
  and REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(raw_tbl.content, "$.brand"), ".+") is not null
  and REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(raw_tbl.content, "$.platform"), ".+") is not null
  and REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(raw_tbl.content, "$.nameplate_display"), ".+") is not null

;