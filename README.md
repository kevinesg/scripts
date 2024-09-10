# Scripts
## Folder Structure
````
.
├── LICENSE
├── README.md
├── etl
│   ├── batch
│   │   ├── finance.py
│   │   ├── nyc_opendata_fhv.py
│   │   ├── openweather.py
│   │   ├── ph_news.py
│   │   ├── schema
│   │   │   ├── finance__ledger.json
│   │   └── usgs_earthquake.py
│   └── stream                                  # currently empty folder
├── misc
│   ├── alerts.py
│   ├── cleanup_airflow_logs.py
│   ├── combine_dbt_data_catalog_files.py
│   └── upload_to_gcs_bucket.py
├── requirements-batch.txt
└── requirements-misc.txt
````
Some files and folders might be missing because they are included in `.gitignore` for privacy purposes. The current scripts are just in their initial working state and will most likely be refactored soon.
##
Let me know if you have any questions! You can contact me at kevinlloydesguerra@gmail.com.