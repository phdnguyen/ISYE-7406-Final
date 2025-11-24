# ISYE-7406-Final

## Environment Set-up

conda env create -f environment.yml

## How to run

Run src/00_setup to download raw call data and hourly seattle weather data to data/raw

Run src/01_preprocessing/preprocessing.ipynb notebook to clean and process raw data, and output to data/processed

Finally, run any of the models in src/02_models/. xgb_5foldCV.ipynb also outputs all stages and metadata to src/models for further development and implementation.