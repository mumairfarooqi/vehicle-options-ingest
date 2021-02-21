# vehicle-options-ingest

This project is about the enrichment of base dataset with the production cost for each record to calculate the profit
per option on each vehicle.

The project has been implemented leveraging the Google Cloud Composer (Airflow) which is a fully managed data workflow
orchestration service.

-----------------------------------------------------------------------------

The project has been designed and implemented in 5 layers:

1. Cloud Storage Layer (The incoming source files arrive here) - GCS Bucket.
2. Raw Data Layer - (All data coming in the files gets staged inside BQ in Semi Structured Json Format) - BigQuery
3. Error Data Layer - (The layer created for capturing any erroneous data and routing it to error tables in BQ.
   Here data validation happens by applying sanity checks) - BigQuery
4. Structured Data Layer - (All raw data that conforms to basic sanity checks gets routed to this layer to get the 
   structure and receive basic level transformation gets applied on data. i.e. trimming, applying correct datatypes,
   replacing empty with Null, replacing newline chars with a space) - BigQuery
5. Transformation Data Layer - (This is where the actual transformation logic gets applied on the structured data) - 
   BigQuery
-----------------------------------------------------------------------------
