Topic Modeling task

This repository contains the code and data for a project that processes raw JSON data into CSV format and then applies topic modeling techniques to extract themes.

Overview
Raw Data: The initial dataset is provided in JSON format.

Data Processing: The JSON data is converted into CSV format using the data_processing.py script.

Modeling: The modeling.ipynb Jupyter Notebook reads the processed CSV data and performs topic modeling to identify key themes.



Modeling
The Jupyter Notebook (modeling.ipynb) loads the processed data from the csv file.

Topic modeling is applied to extract and analyze coherent themes within the data.

Running the notebook will display the results, including visualizations and key insights derived from the topic modeling analysis.

How to Run
Process the Data:

Run the following command to execute the data processing script:

bash
Копировать
python data_processing.py
This script will read the JSON data, convert it into CSV format.

Run the Modeling Notebook:

Open the modeling.ipynb file and run the cells to see the topic modeling results.

Requirements
Python 3.x

Packages listed in requirements.txt (install them with pip install -r requirements.txt)

Jupyter Notebook

Repository Structure
data_processing.py – Script to convert raw JSON data into CSV format.

modeling.ipynb – Jupyter Notebook for performing topic modeling analysis.

requirements.txt – List of Python dependencies.
