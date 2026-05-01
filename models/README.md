# Application-ready FADS_AI model

This folder contains the application-ready trained FADS_AI model and its metadata.

## Files

```text
fads_ai_final_model.rds
fads_ai_feature_columns.rds
fads_ai_model_metadata.json
fads_ai_feature_dictionary.csv
fads_ai_candidate_distributions.csv
```

## Final model

The main application model is:

```text
fads_ai_final_model.rds
```

This object contains the trained model workflow and associated metadata required for application.

The released model uses:

```text
scenario  = classical
algorithm = xgb
features  = lmom_l1, lmom_l2, lmom_l3, lmom_l4, lmom_t3, lmom_t4
```

The model was trained using the 10K Monte Carlo experiment configuration and is intended for application to new annual-maximum samples.

## Feature columns

The file:

```text
fads_ai_feature_columns.rds
```

contains the predictor names required by the model:

```text
lmom_l1
lmom_l2
lmom_l3
lmom_l4
lmom_t3
lmom_t4
```

These features are computed from the input sample using sample L-moments and L-moment ratios.

## Model metadata

The file:

```text
fads_ai_model_metadata.json
```

contains machine-readable metadata describing:

```text
model name
model version
feature scenario
algorithm
candidate distributions
required feature columns
training source
intended use
limitations
creation timestamp
```

## Feature dictionary

The file:

```text
fads_ai_feature_dictionary.csv
```

describes each predictor used by the released application model.

## Candidate distributions

The file:

```text
fads_ai_candidate_distributions.csv
```

lists the candidate distribution families considered by the classifier:

```text
GEV
GPA
PE3
LN2
LN3
GUM
```

## Use in the application example

The example script:

```text
examples/example_real_application.R
```

loads `fads_ai_final_model.rds`, computes the required classical L-moment descriptors from a new annual-maximum sample, and returns the predicted candidate family and class probabilities when available.

## Interpretation

The model output should be interpreted as model-based support for candidate probability-distribution families.

In real applications, the true generating distribution is unknown. The predicted class and associated probabilities should therefore be used as decision-support information, together with hydrological reasoning, diagnostic plots, uncertainty analysis, and conventional frequency-analysis practice.

## Storage note

The final model file may be large. For long-term archival and citation, the complete release should be deposited in Zenodo or a similar repository.