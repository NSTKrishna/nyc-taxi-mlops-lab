# 008: Training window

Stage: S2 · Date: 2026-11-22 · Author: NSTKrishna · Drafted by agent: yes

## Choice

Train on the 3 months right before the holdout (`WINDOW = {"train_months": 3}` in `pipelines/src/labpipes/defs/training.py`). With 2024-01..04 loaded, the model trains on 2024-01..2024-03 and is tested on 2024-04, the month after the window. The zone-pair distance lookup is built from the same 3 months only, never from the holdout.

## Rejected alternative

One month (2024-03 only). It's the month closest to the holdout, so it's the most like what the model will be asked about next, but it has a third of the data. Zone pairs and hours with few trips get thin, noisy estimates.

## Why

<!-- Student: confirm or rewrite this in your own words, and fill in the numbers. -->
More months give more trips per zone pair and hour, and they cover more variety: different weeks, weather, holidays. That matters most for rare pairs. Testing on the *following* month, rather than a random split of the same months, is what serving will actually face: trips the model has never seen, after its training data.

The cost is that the oldest month can differ from the holdout. January looked different in the S1 measurements (null passenger counts 4.7% vs about 11.9% in later months), and winter traffic may not match April. Evidence from the first training run: holdout MAE ___ min vs the v1 baseline ___ min; slices airport ___, Manhattan ___, outer ___, night ___.

## What would show this was wrong

A shorter window (1 or 2 months) giving a lower holdout MAE on the same holdout; the drift report (S8) showing a large PSI between the window's oldest month and the newest; or one slice (for example airport) clearly worse than with a shorter window. Then shorten the window, or weight recent months more.
