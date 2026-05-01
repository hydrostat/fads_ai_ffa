# Application example

This folder contains a minimal example showing how to apply the application-ready **FADS_AI** model to a new annual-maximum sample.

## Files

```text
example_input_annual_maxima.csv
example_real_application.R
```

## Application-ready model

This example uses the trained model distributed in:

```text
models/fads_ai_final_model.rds
```

The released application model uses:

```text
scenario  = classical
algorithm = xgb
features  = lmom_l1, lmom_l2, lmom_l3, lmom_l4, lmom_t3, lmom_t4
```

Therefore, the example computes only classical sample L-moment descriptors. It does not require the user to run the full Monte Carlo pipeline before applying the model.

## Input format

The input file may contain optional metadata lines at the beginning of the file.

Metadata lines must start with `#` and use the format:

```text
# key: value
```

The data table starts after the metadata block and must contain at least one numeric column named:

```text
value
```

The column `value` represents the annual maximum value of the variable being analyzed, such as discharge, precipitation, water level, or another hydrological extreme.

An optional `year` column may be included.

Example:

```text
# station_code: 00000000
# station_name: Example station
# variable: annual maximum discharge
# unit: m3/s
year,value
1981,421.3
1982,358.7
1983,512.4
```

## Requirements for the input sample

The `value` column must:

```text
contain numeric values
use a dot as decimal separator
contain strictly positive values
not contain missing, NaN, or infinite values
contain at least four observations
```

The example is designed for annual-maximum samples. Users are responsible for ensuring that the input series is hydrologically appropriate for frequency analysis.

## Running the example

From the repository root, run:

```r
source("examples/example_real_application.R")
```

The script will:

```text
1. read metadata, if provided;
2. read the annual-maximum sample;
3. compute classical sample L-moment descriptors;
4. load the application-ready FADS_AI model;
5. predict the candidate distribution with highest model-based support;
6. save local output files under outputs/example_application/.
```

## Generated example outputs

The example writes outputs to:

```text
outputs/example_application/
```

Typical files include:

```text
example_application_metadata.csv
example_application_input_data.csv
example_application_features.csv
example_application_prediction.csv
example_application_probabilities.csv
```

These files are generated locally and are not intended to be committed to GitHub.

## Interpretation

In real applications, the true generating distribution is unknown. Therefore, FADS_AI predictions should be interpreted as model-based support for candidate families, not as proof that a particular distribution is the true distribution.

The output probabilities should be considered together with hydrological reasoning, diagnostic plots, uncertainty analysis, and conventional frequency-analysis practice.