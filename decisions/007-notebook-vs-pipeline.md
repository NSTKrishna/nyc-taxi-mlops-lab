# 007: What stays in the notebook, and what moves into the pipeline

Stage: S2 · Date: 2026-11-22 · Author: NSTKrishna · Drafted by agent: yes

## Choice

The notebook (`notebooks/explore.ipynb`, on the SageMaker instance) is for exploration only: looking at the data in Redshift, and comparing the static median, the v1 baseline and v2 LightGBM side by side. Everything that produces a model riders can get lives in the Dagster pipeline (`pipelines/src/labpipes/defs/training.py`): building the training set, training, registering in MLflow with the contract's tags, evaluating against the champion or v1, the blocking gate, and setting `challenger`. The notebook never registers a model and never moves an alias.

## Rejected alternative

The notebook tunes and the pipeline trains: hyperparameter search and feature experiments in the notebook, with the winning parameters copied by hand into the pipeline, which still trains, evaluates and gates every version.

## Why

<!-- Student: confirm or rewrite this in your own words. -->
A model that reaches riders has to be reproducible and checked the same way every time: same window rule, same holdout, same gate, same tags (`training_window`, `git_sha`, ...) that say exactly what produced it. Notebook cells run in whatever order someone ran them, against whatever state the kernel had, so a result from a notebook can't be traced or repeated. Keeping the notebook out of registration also keeps the gate the only path to `challenger`. Hand-copying tuned parameters would add a manual step that can drift (a value copied wrong, or tuned on the holdout month), for a gain that v2's defaults already make unnecessary: v2 beats v1 by 0.317 min of MAE.

The cost: trying a new idea means a pipeline change and a run, not a notebook cell, so iteration is slower.

## What would show this was wrong

Needing several pipeline runs just to try ideas (slow iteration), or a model change that only the notebook could have found. Then move tuning into the pipeline as its own asset (search over parameters on the window, still evaluated and gated), rather than copying numbers by hand.
