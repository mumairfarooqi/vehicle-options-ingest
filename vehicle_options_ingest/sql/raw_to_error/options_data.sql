Select * from (

SELECT

case when JSON_EXTRACT_SCALAR(raw_tbl.content, "$.Option_Desc") like '%\n%' then concat(raw_tbl.content,'|','column contains newline char') end as error_record_with_reason,
current_timestamp() as load_ts

FROM
  `breuninger-datalake-dev.airflow_playground.options_data_raw` raw_tbl

) result

where result.error_record_with_reason is not null;
