# 5minPATH Dashboard

This is a lightweight read-only showcase dashboard for the `5minPATH` research tool.

It does not run capture, replay, calibration, or shadow measurement. It only presents the main project result in a reviewer-friendly format.

## Run

```bash
python -m pip install streamlit pandas
streamlit run dashboard/app.py
```

## Scope

- read-only
- sample/evidence driven
- no trading
- no mutation of artifacts
- no changes to research logic

## What It Shows

- what `5minPATH` is
- what the project tested
- what the six clean-shadow days showed
- why the tested strategy was not consistently deployment-effective
- why the tool remains valuable as a research/data pipeline
