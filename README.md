# Phase 1 Financial Data Platform

## High Level

We will use **ThetaData** as our data provider (to start). They offer an API that can be used to fetch both historical and live data.  
We will create two containerized services using Docker on our end:
- One for fetching data
- One for subscribing to the live data stream

These services will lightly process data as needed and write the data to a **RedPanda** topic (or Kafka if RedPanda proves too complex).  
All of this can be coordinated with `docker` + `docker-compose` locally.

---

## Requirements

### Historical Stock Data Service
- Container will run the **Theta Client `.jar`** file (found under Downloads on ThetaData).
- Add functionality to:
  - Accept a `backtest_id`, a `ticker`, and a `date range`
  - Return the formatted historical stock data
- Lightly process the data as it is returned from the API
- Enforce a schema using **Avro**
- Write the data to the topic using the `confluent-kafka` Python library
- Expose a **POST** API endpoint for our main service to request data

### Live Stock Data Service
- Container will also run the **Theta Client `.jar`** file
- Add functionality to:
  - Subscribe to the live data stream
  - Return formatted stock data
- Match the schema of the historical data (no processing required)
- Write directly to the topic with schema enforcement
- Expose a **POST** API endpoint for our main service to request data

### Historical Option Data Service
- Container will also run the **Theta Client `.jar`** file
-
## Coordinator API

Each coordinator will expose an API to specify what data to retrieve.

- Example: Run a backtest on AAPL Options
- What we need for data:
    - 
  - Which tickers
  - Desired interval
  - Associated backtest ID  
- Return resulting data from the API
- Use **FastAPI** to expose POST endpoints for historical and live data
- Include logic to:
  - Process incoming requests
  - Instruct the data services to fetch and write the appropriate data to the topic

## RedPanda / Kafka

RedPanda and Kafka are open-source streaming data platforms.

### Producers
- Write data to a topic
- Transfer data from memory to disk via **messages** (or **envelopes**)
- A message is a serialized set of data — in our case, financial information

### Topics
- A **topic** is a channel for sending messages
- It is an **append-only log** (similar to appending elements to a Python list)
- We'll define topics based on ticker, schema, source, and backtest ID:

  #### Format:
  - Historical Stock: `stock-<ticker>-<schema>-historical-<backtest_id>`  
    Example: `aapl-ohlcv-historical-1`
  - Live Stock: `stock-<ticker>-<schema>-live`  
    Example: `stock-aapl-ohlcv1m-live`
  - Historical Option: `option-<root>-<schema>-historical-<backtest_id>`  
    Example: `option-spxw-quote-historical-1`
  - Live Stock: `option-<root>-<schema>`  
    Example: `aapl-ohlcv-live`

### Consumers
- Subscribe to a topic and read data in order
- Multiple consumers can subscribe to the same topic
- A consumer is part of a **consumer group**  
  This allows:
  - Writing AAPL data to a topic
  - Group A doing Calculation 1
  - Group B doing Calculation 2 in parallel
