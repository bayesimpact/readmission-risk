# Bayes Impact 30-day patient readmission model

The result of a 12-month project by in collaboration with [Sutter Health](http://www.sutterhealth.org/), funded by [Robert Wood Johnson Foundation](http://www.rwjf.org/).

For questions, contact Mehdi Jamei (mehdi@thebeaconlabs.org).

## Summary of the repo

This repo contains the parameters for a neural network used to predict 30-day all-cause hospital readmissions. Later in this README you'll find some example code in Python to load and use the model on a properly-formatted dataset of patients to obtain probabilities of their readmission.

This repo also contains the code we at Bayes used to train and evaluate our models, as well the code to our pipeline to extract, transform, and load the raw data given to us by Sutter Health.

Our primary goal in sharing the full codebase and model parameters is to show other researchers how we approached the problem. While it's possible to take our code and port it to another setting, it's a secondary concern.

## Project Background

Sutter Health extracted hospital stay data from their EHR, Epic, for some 300,000 patients between 2009 and 2015. Bayes Impact used this data to explore a range of machine learning models to predict 30-day readmission risk, using all data available at the time of discharge. The work involved exploratory data analysis of the patient population, feature engineering and evaluation of different types of features, and experimenting with a range of models and model parameters. The best model in the end was a 2-layer neural network, the parameters of which can be found in this repo.

## Repo contents / key files

- `/features/features_*.txt` – Descriptions of the 1667 features used by the model, as well as the 100-feature and 500-feature subsets.
- `/features/statistics.ipynb` – [Jupyter](http://jupyter.org/) notebook showing summary statistics for each feature.
- `/features/extraction/` – Example feature extraction pipeline that can be adapted to extract this set of features from an EMR system.
- `/model/*.h5` – Neural network model weights, in HDF5 format, for the full model, as well as the 100-feature and 500-feature reduced models.
- `/model/*.json` – Neural network model structures, in JSON format, for the full model, as well as the 100-feature and 500-feature reduced models.

## Pipeline

Below is the rough pipeline of data flow and work represented here. The code for all of these steps (except for Sutter's internal data extraction) can be found in `features/extraction/`

![Data pipeline](https://github.com/bayesimpact/readmission-risk/blob/master/doc/images/data-pipeline.png?raw=true)

### Model loading and prediction (sample Python/Jupyter code)

```python
%env KERAS_BACKEND=tensorflow
import keras
from keras.models import model_from_json
import pandas as pd

model = model_from_json(open('model.json').read())
model.load_weights('model.h5')

# features.csv should be a csv file with columns in the order
# described in features_100.txt, features_500.txt, or features_full.txt.
patients = pd.read_csv('features.csv', index_col=0)
predictions = model.predict_proba(patients.values)[:, 0]
```

