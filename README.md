# ARCHIVED
This repository has been **archived**. Another repository now contains most of the content and functionality that was once here. The new repository will receive any updates and fixes, while this one will not.

Link to NEW REPOSITORY: https://github.com/igorcmvaz/future-EPW-analysis

## Old README
### EPW Analysis
A script to process multiple [EPW files](https://climate.onebuilding.org/papers/EnergyPlus_Weather_File_Format.pdf) into a single [(Apache) Parquet file](https://parquet.apache.org/docs/file-format/), taking only the desired data columns and informational fields.

#### How to use
```bash
git clone git@github.com:Matbbastos/epw-analysis.git
python -m pip install -r requirements.txt

python -m merge_files_into_parquet.py -h
python -m merge_files_into_parquet.py sample_input --csv
```

#### Confort Models
Calculated variables are introduced in the files based on the models from `pythermalcomfort`. For more information, visit [their documentation site](https://pythermalcomfort.readthedocs.io/en/latest/reference/pythermalcomfort.html#comfort-models). The models are included in the exported files in an 'opt-out' format, so that passing the `-s` or `--strict` option will prevent `pythermalcomfort` from being imported, skip computation of the models and exclude corresponding entries from the output files.

**Note:** Bear in mind that merely importing `pythermalcomfort` comes with a significant overhead for the script, and the computation of the values for comfort models are executed in each iteration, also noticeably impacting overall performance.  For that reason, the imports are conditional and only executed if the models are in fact desired (no `--strict` option).

An extra option (`-l` or `--limit-utci`) was added to the script, and it is used to configure the flag of similar name in UTCI model, as detailed in the [corresponding documentation](https://pythermalcomfort.readthedocs.io/en/latest/reference/pythermalcomfort.html#universal-thermal-climate-index-utci) (check parameter `limit_inputs`). If this option is present, this flag is used when computing UTCI models and the **Wind Speed values** from EPW files are saturated using the lower/upper bounds from the model (as per documentation), to ensure they are within the working range. Default is to not limit the inputs and use extrapolation from the UTCI model itself (rather than saturating the values).

### Commits
When committing to this repository, following convention is advised:

* chore: regular maintenance unrelated to source code (dependencies, config, etc)
* docs: updates to any documentation
* feat: new features
* fix: bug fixes
* ref: refactored code (no new feature or bug fix)
* revert: reverts on previous commits
* test: updates to tests

For further reference on writing good commit messages, see [Conventional Commits](https://www.conventionalcommits.org).
