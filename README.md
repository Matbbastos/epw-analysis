# EPW Analysis
A script to process multiple [EPW files](https://climate.onebuilding.org/papers/EnergyPlus_Weather_File_Format.pdf) into a single [(Apache) Parquet file](https://parquet.apache.org/docs/file-format/), taking only the desired data columns and informational fields.

## How to use
```bash
git clone git@github.com:Matbbastos/epw-analysis.git
python -m pip install -r requirements.txt

python -m merge_files_into_parquet.py -h
python -m merge_files_into_parquet.py sample_input --csv
```

# Commits
When committing to this repository, following convention is advised:

* chore: regular maintenance unrelated to source code (dependencies, config, etc)
* docs: updates to any documentation
* feat: new features
* fix: bug fixes
* ref: refactored code (no new feature or bug fix)
* revert: reverts on previous commits
* test: updates to tests

For further reference on writing good commit messages, see [Conventional Commits](https://www.conventionalcommits.org).
