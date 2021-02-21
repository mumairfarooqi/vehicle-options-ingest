select * from (

SELECT

case when REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(raw_tbl.content, "$.VIN"), ".+") is null or REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(raw_tbl.content, "$.Sales_Price"), ".+") is null
then concat(raw_tbl.content,'|','single col or multiple cols having null')
end as error_record_with_reason,
current_timestamp() as load_ts

FROM
  `breuninger-datalake-dev.airflow_playground.base_data_raw` raw_tbl

) result

where result.error_record_with_reason is not null;