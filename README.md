# Bayes Impact 30-day patient readmission model

## Introduction

TODO

## Contents

- `/features/features_*.txt` (TODO) – Descriptions of the 1667 features used by the model, as well as the 100-feature and 500-feature subsets.
- `/features/statistics.ipynb` (TODO) – Jupyter notebook showing summary statistics for each feature.
- `/features/extraction/` – Example feature extraction pipeline that can be adapted to extract this set of features from an EMR system.
- `/model/*.h5` – Neural network model weights, in HDF5 format, for the full model, as well as the 100-feature and 500-feature reduced models.
- `/model/*.json` – Neural network model structures, in JSON format, for the full model, as well as the 100-feature and 500-feature reduced models.

## Usage

### Prerequisites

TODO

### Feature extraction

![Data pipeline](https://github.com/bayesimpact/readmission-risk/blob/master/doc/images/data-pipeline.png?raw=true)

TODO

### Model loading and prediction

TODO

```
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

### Validation

TODO
