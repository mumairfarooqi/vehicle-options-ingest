Select * from (

SELECT case when

REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(raw_tbl.content, "$.nameplate_code"), ".+") is  null
  or REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(raw_tbl.content, "$.brand"), ".+") is  null
  or REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(raw_tbl.content, "$.platform"), ".+") is  null
  or REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(raw_tbl.content, "$.nameplate_display"), ".+") is  null


then concat(raw_tbl.content,'|','masterdata row contains null column') end as error_record_with_reason,
current_timestamp() as load_ts

FROM
  `breuninger-datalake-dev.airflow_playground.vehicle_line_mapping_raw` raw_tbl

) result

where result.error_record_with_reason is not null;
