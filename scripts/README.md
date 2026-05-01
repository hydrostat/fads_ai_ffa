# Core scripts

This folder contains the main reproducible pipeline scripts.

The intended sequence is:

```text
000_config.R
001_create_project_structure.R
010_simulate_samples.R
020_extract_features.R
021_extract_gof_features.R
022_combine_gof_chunks.R
030_build_datasets.R
040_train_models.R
050_evaluate_models.R
900_run_full_pipeline.R
```

The runner script `900_run_full_pipeline.R` should be used only after confirming local paths and computational resources.
