# FADS_AI

**FADS_AI** (*Flood-frequency Analysis Distribution Selection with Artificial Intelligence*) is a reproducible code base and application-ready model for machine-learning support in probability-distribution selection for Flood Frequency Analysis (FFA).

This repository is maintained within the **hydro_stat** research group.

## Purpose

This repository provides:

```text
1. source code to reproduce the computational workflow associated with a Monte Carlo study on AI-assisted probability-distribution selection in FFA;
2. an application-ready trained model for applying FADS_AI to new annual-maximum samples.
```

The framework treats distribution selection as a supervised multiclass classification problem under controlled simulation conditions. Candidate distributions include:

- Generalized Extreme Value (GEV)
- Generalized Pareto (GPA)
- Pearson type III (PE3)
- Two-parameter lognormal (LN2)
- Three-parameter lognormal (LN3)
- Gumbel (GUM)

The framework is intended as a decision-support approach. It does not replace hydrological reasoning, goodness-of-fit diagnostics, uncertainty analysis, or expert judgement.

## Repository structure

```text
R/                                      Reusable R helper functions
scripts/                                Core reproducible pipeline scripts
scripts/supplementary_analysis/         Supplementary analysis scripts
paper_outputs/tables/scripts/           Scripts used to generate manuscript tables
paper_outputs/figures/scripts/          Scripts used to generate manuscript figures
paper_outputs/quantile_consequence/     Quantile-consequence preparation scripts
data/input_data/                        Fixed auxiliary input files required by the pipeline
data/sample_inputs/                     Small sample inputs for examples
examples/                               Minimal application example
models/                                 Application-ready trained model and metadata
outputs/                                Generated outputs, not versioned by default
tests/                                  Integrity checks and lightweight tests
```

## Reproducibility and distribution policy

Precomputed intermediate outputs from the main Monte Carlo experiment are not distributed with this repository.

Users can regenerate simulation metadata, feature tables, goodness-of-fit tables, datasets, training artifacts, evaluation outputs, manuscript tables, and manuscript figures by running the reproducibility pipeline.

Raw Monte Carlo samples are not stored as static files. Samples are regenerated deterministically from the simulation grid, distribution family, parameter values, sample size, and random seed.

The application-ready final trained model and its metadata are distributed in the `models/` folder or through the archived release. This model is distinct from intermediate outputs generated during the reproducibility pipeline.

## Application-ready trained model

The repository provides an application-ready FADS_AI model:

```text
models/fads_ai_final_model.rds
```

The released application model uses:

```text
scenario  = classical
algorithm = xgb
features  = lmom_l1, lmom_l2, lmom_l3, lmom_l4, lmom_t3, lmom_t4
```

This model is intended for practical reuse on new annual-maximum samples. Its predictions should be interpreted as model-based support for candidate distribution families, not as definitive identification of the true generating distribution.

Additional model metadata are provided in:

```text
models/fads_ai_feature_columns.rds
models/fads_ai_model_metadata.json
models/fads_ai_feature_dictionary.csv
models/fads_ai_candidate_distributions.csv
```

## Fixed input file

The repository includes one fixed auxiliary input file:

```text
data/input_data/poligonos_pre.rds
```

This object contains preprocessed L-moment Ratio Diagram (LMRD) polygon information required to compute LMRD-based features in the full reproducibility pipeline.

The application-ready classical model does not require LMRD features, but the file is required for reproducing the full study workflow.

## Requirements

The pipeline is written in R.

Required R packages include:

```text
arrow
data.table
dplyr
future
future.apply
goftest
kernlab
lmom
nnet
parsnip
purrr
Rcpp
ranger
readr
recipes
rpart
tibble
tidymodels
tidyr
tidyselect
workflows
xgboost
yardstick
```

Because the LMRD point-in-polygon routine uses `Rcpp`, Windows users may need Rtools installed.

## Running the application example

The application example does not require the full Monte Carlo pipeline to be rerun.

From the repository root, run:

```r
source("examples/example_real_application.R")
```

The example reads:

```text
examples/example_input_annual_maxima.csv
```

The input file must contain a numeric column named `value`, representing the annual maximum value of the variable being analyzed.

Example:

```text
year,value
1981,421.3
1982,358.7
1983,512.4
```

Example outputs are written locally to:

```text
outputs/example_application/
```

## Running the full reproducibility pipeline

Set the working directory to the repository root:

```r
setwd("path/to/fads_ai_ffa")
```

Then run:

```r
source("scripts/900_run_full_pipeline.R")
```

To force a clean run from the beginning:

```r
options(fads.run_mode = "clean")
source("scripts/900_run_full_pipeline.R")
```

The full Monte Carlo experiment may be computationally demanding. Users should inspect:

```text
scripts/000_config.R
```

before running the complete workflow.

## Pipeline order

The main reproducibility workflow is:

```text
001_create_project_structure.R
000_config.R
010_simulate_samples.R
020_extract_features.R
021_extract_gof_features.R
022_combine_gof_chunks.R
030_build_datasets.R
040_train_models.R
050_evaluate_models.R
```

The master runner `scripts/900_run_full_pipeline.R` executes these stages in sequence, with resume and clean-run support.

## Active model configuration

The public reproducibility configuration defines the active model set in:

```text
scripts/000_config.R
```

The code base supports multiple model specifications, but the active models used in a given run are determined by the configuration file.

## Validation and test evaluation

Model selection in the reproducibility pipeline is performed using validation-set performance. The held-out test set is used only for final evaluation of validation-selected model-scenario combinations.

The training script may store minimal model markers for selected model-scenario combinations. The final evaluation script detects these markers and retrains the selected models in memory before test-set evaluation.

## Generated outputs

Generated intermediate outputs from the reproducibility pipeline are written under:

```text
outputs/
```

These files are not intended to be committed to GitHub. They include generated simulation metadata, feature tables, GOF/IC tables, datasets, trained-model markers, evaluation summaries, logs, and manuscript-output files.

## Data availability

See:

```text
data-availability.md
```

## Reproducibility notes

See:

```text
reproducibility.md
```

## Citation

Please cite the archived software release:

```text
Fernandes, W. (2026). FADS_AI: Machine-learning support for probability-distribution selection in flood frequency analysis (v1.0.0). Zenodo. https://doi.org/10.5281/zenodo.19950473
```

## License

The source code is released under the MIT License unless otherwise noted.