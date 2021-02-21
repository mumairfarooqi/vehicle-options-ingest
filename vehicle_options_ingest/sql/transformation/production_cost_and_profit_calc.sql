SELECT   result.*,
         cast(result.sales_price - result.production_costs AS numeric) AS profit
FROM     (
                   SELECT    bd.vin,
                             bd.option_quantities,
                             bd.options_code,
                             bd.option_desc,
                             bd.model_text,
                             bd.sales_price,
                             cast(
                             CASE
                                       WHEN bd.sales_price <= 0 THEN 0                                                                                               -- (1) Case Logic
                                       WHEN bd.sales_price > 0
                                       AND       od.option_code IS NOT NULL
                                       AND       od.model IS NOT NULL THEN od.material_cost                                                                          -- (2) Case Logic
                                       WHEN bd.sales_price > 0
                                       AND       od.model IS NULL
                                       AND       od.option_code IS NULL
                                       AND       bd.options_code IN
                                                                     (
                                                                     SELECT DISTINCT option_code
                                                                     FROM            `breuninger-datalake-dev.airflow_playground.options_data_structured`) THEN
                                                 (
                                                        SELECT avg(corr_sub.material_cost)
                                                        FROM   (
                                                                        SELECT   *
                                                                        FROM     `breuninger-datalake-dev.airflow_playground.options_data_structured` od_in
                                                                        WHERE    od_in.option_code=bd.options_code
                                                                        GROUP BY 1,
                                                                                 2,
                                                                                 3,
                                                                                 4 )corr_sub )                                                                       -- (3) Case Logic
                                       WHEN bd.sales_price > 0
                                       AND       od.model IS NULL
                                       AND       od.option_code IS NULL
                                       AND       bd.options_code NOT IN
                                                                         (
                                                                         SELECT DISTINCT option_code
                                                                         FROM            `breuninger-datalake-dev.airflow_playground.options_data_structured`)
                                                                         THEN bd.sales_price*0.45                                                                    -- (4) Case Logic
                             end AS NUMERIC) AS production_costs
                   FROM      `breuninger-datalake-dev.airflow_playground.base_data_structured` bd
                   LEFT JOIN `breuninger-datalake-dev.airflow_playground.options_data_structured` od
                   ON        bd.options_code = od.option_code
                   AND       bd.model_text = od.model ) result                                                                                                       -- Result Set for Profit Calc
ORDER BY result.vin ;
