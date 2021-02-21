SELECT

  trim(REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(raw_tbl.content, "$.VIN"), ".+")) as vin,
  CAST(REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(raw_tbl.content, "$.Option_Quantities"), ".+") AS INT64) as option_quantities,
  trim(REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(raw_tbl.content, "$.Options_Code"), ".+")) as options_code,
  trim(REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(raw_tbl.content, "$.Option_Desc"), ".+")) as option_desc,
  trim(REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(raw_tbl.content, "$.Model_Text"), ".+")) as model_text,
  CAST(REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(raw_tbl.content, "$.Sales_Price"), ".+") AS NUMERIC) as sales_price

FROM
  `breuninger-datalake-dev.airflow_playground.base_data_raw` raw_tbl

  where REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(raw_tbl.content, "$.VIN"), ".+") is not null and
  REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(raw_tbl.content, "$.Sales_Price"), ".+") is not null
;
