## **Arithmetic Operators**

| Operator                            | Description                                                                         |
| ----------------------------------- | ----------------------------------------------------------------------------------- |
| `abs(x)`                            | Absolute value of x                                                                 |
| `add(x, y, filter=false)`           | Add inputs. If `filter=true`, NaNs are filtered to 0 before adding                  |
| `densify(x)`                        | Converts grouping field of many buckets into fewer available buckets for efficiency |
| `divide(x, y)`                      | x / y                                                                               |
| `inverse(x)`                        | 1 / x                                                                               |
| `log(x)`                            | Natural logarithm                                                                   |
| `max(x, y, ...)`                    | Maximum of inputs                                                                   |
| `min(x, y, ...)`                    | Minimum of inputs                                                                   |
| `multiply(x, y, ..., filter=false)` | Multiply inputs. If `filter=true`, NaNs treated as 1                                |

---

## **Logical Operators**

| Operator                       | Description                                         |
| ------------------------------ | --------------------------------------------------- |
| `power(x, y)`                  | x ^ y                                               |
| `reverse(x)`                   | -x                                                  |
| `sign(x)`                      | Returns 1 if x>0, -1 if x<0, 0 if x=0, NaN if x=NaN |
| `signed_power(x, y)`           | x^y preserving sign of x                            |
| `sqrt(x)`                      | Square root of x                                    |
| `subtract(x, y, filter=false)` | x − y. If `filter=true`, NaNs treated as 0          |
| `and(a, b)`                    | Logical AND                                         |
| `or(a, b)`                     | Logical OR                                          |
| `not(x)`                       | Logical negation                                    |
| `if_else(cond, x, y)`          | If cond true → x else → y                           |
| `<, <=, >, >=, ==, !=`         | Standard comparison operators                       |

---

## **Time Series Operators**

| Operator                                   | Description                                     |
| ------------------------------------------ | ----------------------------------------------- |
| `is_nan(x)`                                | Returns 1 if x is NaN else 0                    |
| `days_from_last_change(x)`                 | Days since last change in x                     |
| `hump(x, hump=0.01)`                       | Limits magnitude of changes (reduces turnover)  |
| `kth_element(x, d, k)`                     | K-th valid value within lookback d              |
| `last_diff_value(x, d)`                    | Last value different from current within d days |
| `ts_arg_max(x, d)`                         | Index of max value in last d days               |
| `ts_arg_min(x, d)`                         | Index of min value in last d days               |
| `ts_av_diff(x, d)`                         | x − ts_mean(x,d), NaNs ignored                  |
| `ts_backfill(x, d)`                        | First valid value in lookback d                 |
| `ts_corr(x, y, d)`                         | Correlation over last d days                    |
| `ts_count_nans(x, d)`                      | Count NaNs in last d days                       |
| `ts_covariance(x, y, d)`                   | Covariance over last d days                     |
| `ts_decay_linear(x, d, dense=false)`       | Linear decay over lookback                      |
| `ts_delay(x, d)`                           | Value d days ago                                |
| `ts_delta(x, d)`                           | x − ts_delay(x,d)                               |
| `ts_mean(x, d)`                            | Mean over last d days                           |
| `ts_product(x, d)`                         | Product over last d days                        |
| `ts_quantile(x, d, driver="gaussian")`     | Quantile transform using distribution           |
| `ts_rank(x, d, constant=0)`                | Rank of current value in last d days            |
| `ts_regression(y, x, d, lag=0, rettype=0)` | Regression statistics                           |
| `ts_scale(x, d, constant=0)`               | Time-series min-max scaling                     |
| `ts_std_dev(x, d)`                         | Standard deviation                              |
| `ts_step()`                                | Day counter                                     |
| `ts_sum(x, d)`                             | Sum over last d days                            |
| `ts_zscore(x, d)`                          | Time-series z-score                             |

---

## **Cross Sectional Operators**

| Operator                                       | Description                      |
| ---------------------------------------------- | -------------------------------- |
| `normalize(x, useStd=false, limit=0.0)`        | Cross-sectional de-mean          |
| `quantile(x, driver="gaussian", sigma=1.0)`    | Rank → distribution transform    |
| `rank(x, rate=2)`                              | Rank across instruments (0 to 1) |
| `scale(x, scale=1, longscale=1, shortscale=1)` | Scale to book size               |
| `winsorize(x, std=4)`                          | Clip outliers beyond std limits  |
| `zscore(x)`                                    | Cross-sectional z-score          |

---

## **Vector Operators**

| Operator     | Description          |
| ------------ | -------------------- |
| `vec_avg(x)` | Mean of vector field |
| `vec_sum(x)` | Sum of vector field  |

---

## **Group Operators**

| Operator                               | Description                               |
| -------------------------------------- | ----------------------------------------- |
| `group_backfill(x, group, d, std=4.0)` | Backfill NaNs using winsorized group mean |
| `group_mean(x, weight, group)`         | Replace values by group mean              |
| `group_neutralize(x, group)`           | Neutralize alpha against group            |
| `group_rank(x, group)`                 | Rank within group                         |
| `group_scale(x, group)`                | Min-max scale within group                |
| `group_zscore(x, group)`               | Z-score within group                      |

