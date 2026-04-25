# 5minPATH Dashboard

This is an optional read-only showcase dashboard for the completed `5minPATH` research/data pipeline.

It does not run capture, replay, calibration, shadow measurement, artifact mutation, or trading execution. It only presents the main project result in a reviewer-friendly format.

## Run Locally

```bash
python -m pip install -e '.[dashboard]'
streamlit run dashboard/app.py
```

Alternative minimal install:

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
