# ISYE-7406-Final

## Environment Set-up

conda env create -f environment.yml

## How to run

Run 00_setup to download raw call data and hourly seattle weather data to data/raw

Run 01_preprocessing/preprocessing.ipynb notebook to clean and process raw data, and output to data/processed

Finally, run any of the models in 02_models/ 