# Machine learning: task-duration prediction with XGBoost

Code: `ml/xgb_model.{h,cpp}` (XGBoost wrapper), `ml/random_forest.{h,cpp}` (alternative), `ml/duration_model.h` (which model is active), `ml/features.h`, `ml/trainer.{h,cpp}` (pipeline), `master/ml_manager.{h,cpp}`, `tools/nexus_ml.cpp`, tests `tests/test_ml.cpp`.

## 1. Is it really XGBoost?

Yes. `ml/xgb_model.cpp` calls the **official XGBoost C API** (`XGBoosterCreate`, `XGBoosterUpdateOneIter`, `XGBoosterPredictFromDMatrix`, `XGBoosterSaveModelToBuffer` …) of `libxgboost.so` built from the `dmlc/xgboost` tag **v2.1.4** by `scripts/install_xgboost.sh`.
CMake defines `NEXUS_HAVE_XGBOOST` only when the library is found. Without it, `XgbRegressor::available()` is `false`, XGBoost training **fails with an explicit message** and the dashboard shows *XGBoost library: not built in* – the Random Forest is never presented under the XGBoost name.
The Random Forest is still available as a separate model (`model_type=random_forest`), clearly labelled everywhere (dashboard, API `model_kind`, `ml_training_runs.model_type`, model file header `NEXUSXGB` vs `NEXUSRF`).

Settings: objective `reg:squarederror`, `tree_method=hist`, 150 rounds, `max_depth=4`, `eta=0.1`, λ=1, seed 42, **`nthread=1` so training is deterministic** (identical data ⇒ identical model; tested). Override with `NEXUS_ML_XGB_ROUNDS / _DEPTH / _ETA`.

## 2. Label and features

* **Label:** `execution_history.execution_time` (seconds, measured on the Worker) of **COMPLETED** executions.
* **Features** (`ml/features.h`) – 16 numbers, all known **when the scheduler decides**:
  one-hot task type (8), `log2(scale)`, `log10(1 + input_bytes)`, the candidate Worker's `cpu %`, `ram %`, `running` tasks, `capacity`, `cores`, and `occupancy = running/capacity`.
  CPU/RAM/running are the values **at assignment time** (`worker_cpu_at_assign`, `worker_ram_at_assign`, `running_at_assign`, `worker_capacity` columns), not at completion.
* **No leakage:** nothing observed after the task ran is a feature; a per-task-type *mean duration* is deliberately excluded (it is a function of the targets); a unit test asserts that no feature name mentions duration/exec/result/finish, and another that training on pure noise does **not** produce a near-zero held-out error (which would prove the test rows were seen).

## 3. Training and evaluation

`trainFromDatabase()` → `trainAndEvaluate()`:

1. read all qualifying rows from PostgreSQL;
2. fewer than `NEXUS_ML_MIN_SAMPLES` (30, and never < 10) → status **`INSUFFICIENT_DATA`**: no model, no file, no invented number;
3. seeded Fisher–Yates shuffle; **80 % train / 20 % held-out test** (at least 3 test rows); the model never sees the test rows;
4. fit on the train rows only;
5. **MAE and RMSE on the held-out rows**, next to the **baseline** "always predict the training mean";
6. the other algorithm (Random Forest for an XGBoost run, and vice versa) is fitted on the *same* split and its MAE/RMSE stored as `compare_*` – so the GUI can show XGBoost vs Random Forest vs baseline honestly;
7. if MAE < baseline MAE → **`TRAINED`**, model saved and hot-swapped into the running Master; otherwise **`NOT_ACTIVATED`** and the scheduler keeps its fallback.

Every run is a row in `ml_training_runs` (status, samples, split sizes, MAE, RMSE, baseline, algorithm, hyper-parameters, comparison, message). Dashboard → *AI model* lists them.
The held-out numbers are only as meaningful as the history: with a handful of task types and one host the error is small because the workloads are regular; that shows the pipeline works, not that arbitrary tasks can be predicted.

## 4. Persistence and reloading

File `models/duration_xgb.model` (or `duration_rf.model`; `NEXUS_ML_MODEL` overrides): `RUN <run id>` line, a `NEXUSXGB 1 <features> <rounds>` header and the **XGBoost JSON model**. Written to a temp file and renamed. At start `MlManager::loadFromDisk()` loads it; a missing, corrupt or wrong-feature-count file is refused (the scheduler then falls back). Round-trip equality of predictions is tested.

## 5. Scheduler integration

`MlManager::predictor()` returns a function `(task, candidate Worker) → seconds` that builds the 16 features from the **candidate's current metrics** and calls the model. `Scheduler::pick()` (AI_ASSISTED) asks it for every usable candidate and takes the minimum. If there is no model, or any candidate cannot be predicted, the decision falls back to Resource-Aware and is stored with `fallback=true`.
Each used prediction is written to `ml_predictions`; when the task completes `actual_seconds` is filled in, which gives *live* MAE/RMSE of predicted vs actual (dashboard, benchmarks).

## 6. (Re)training without blocking tasks

* Manual: dashboard buttons (*Train / retrain XGBoost*, *Train Random Forest instead*), `POST /api/ml/train`, `./scripts/train_model.sh`, `nexus_ml train [xgboost|random_forest]`.
* Automatic: a background thread (`MlManager::startAutoRetrain`, `NEXUS_ML_AUTO_RETRAIN_EVERY`, default 100) retrains after that many *new* completed executions. Training runs on its own thread; the dispatcher keeps using the previous model until the new one is swapped in atomically (a `shared_ptr` replace). One training run at a time (`trainMutex_`).

## 7. Commands

```bash
./scripts/install_xgboost.sh            # once
./scripts/build.sh
./scripts/train_model.sh                # XGBoost, through the running Master
./scripts/train_model.sh --status
./build/nexus_ml train random_forest    # offline, other algorithm
./build/nexus_ml predict CPU_BURN 4 20 30 1 1 12      # type scale cpu% ram% running capacity cores [input_bytes]
```
Measured numbers from real runs are in [FINAL_TEST_REPORT.md](FINAL_TEST_REPORT.md).
