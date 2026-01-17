## (Proposed) Overview

Create an API that fetch data from AWS S3, transform it into specific format relating to the API called, and transfer it to the local machine, package the API into PyPI. By entering AWS key and security, it's able to use this API.

## Storage structure

In AWS S3, bucket us-equity-datalake, the data is stored in 

```
/data
    /derived/features/fundamental/{cik}/
        /metrics.parquet
        /ttm.parquet
    /master
        /security_master.parquet
    /raw
        /fundamental/{cik}/fundamental.parquet
        /ticks
            /daily/{security_id}/
                /history.parquet
                /{current_year}/{month:02d}/ticks.parquet
            /minute/{security_id}/{month:02d}/{day:02d}/ticks.parquet
    /symbols/{year}/{month:02d}/top3000.txt
```

## Schema


| File          | Format   | Columns   |
| ------ | ------ | ---------------- |
| **ttm.parquet**      | Long table    | symbol, as_of_date, accn, form, concept, value, start, end, frame        |
| **metrics.parquet**     | Long table    | symbol, as_of_date, metric, value    |
| **security_master.parquet** | Long table | security_id, permno, symbol, company, cik, cusip, start_date, end_date        |
| **ticks.parquet** (minute)      |  Long table    | timestamp, open, high, low, close, volume, num_trades, vwap                   |
| **ticks.parquet** (daily). | Long table     | timestamp, open, high, low, close, volume                                     |
| **history.parquet**         | Long table  | timestamp, open, high, low, close, volume                                     |
| **fundamental.parquet**     | Long table     | symbol, as_of_date, accn, form, concept, value, start, end, frame, is_instant |
| **top3000.txt**             | List  |  symbol     |

where

- symbol: like AAPL
- as_of_date: filed date in SEC EDGAR
- accn: access number of documents
- form: like 10-K, 10-Q, 8-K
- concept: economic concepts instead of raw GAAP fields, like "ta" (total assets), "rev" (revenue), etc. Only duration concepts in the income statements have ttm, others are PIT
- metric: similar to concept, but it is derived by concepts. Uses ttm if one component concept is duration variable
- value: the actual raw value of that field
- security_id: the unique identifier of a security, also introduced below
- permno: permenant number assigned by CRSP, it tracks the company, not security (if a company issued two different stocks, both stocks have the same permno)
- company: company name
- cik: unique identifier assigned by SEC. Not all symbols are assigned cik
- cusip: another identifier, check on https://github.com/dafu-zhu/us-equity-datalake/ to see where it is used
- timestamp: For minute ticks it's YYYY-MM-DD HH:MM:SS. For daily ticks it's YYYY-MM-DD
- open: open price
- high: high price
- low: low price
- close: close price
- volume: trading volume
- num_trades: number of trades completed in exchange
- vwap: volume weighted average price


## Transformation

### Security ID

The data fetching is point-in-time. That is, given a time $t$ and a symbol $s$, we can find the security_id $\text{id}_t^s$ which is a unique identifier of security, from the table /master/security_master.parquet. This avoids the messy situation where company rebrands (FB->META), symbol company delisted and recycled, company occurs a split, etc. With `security_id`, we are able to fetch data from ticks, and connect to a unique cik, and further fetch fundamental (raw, ttm, derived) data.

### Output format

Use plan mode to discuss. Consider operational efficiency, latency, costs (frequency of api calling):

- The api will be integrated to downstream alpha research project, suits alpha operators (both sectional and time-series). Preferred format is wide table: row (time), columns (security), value (field). Field include: 
  - daily ticks data: open, close, high, low, volume
  - fundamental: raw fields, income statement fields have an additional ttm version, derived
- Transformation on RAM can cause latency
- Requesting from S3 cost money, balance between size (do not crash the RAM) and number of calls (do not put api calling in loop). Given the current storage structure, how to reduce requesting from S3?

## Clean API structure

1. Begin a session, instantiate a client
2. Handle null value elegantly
3. Use polars for fast processing
4. ...