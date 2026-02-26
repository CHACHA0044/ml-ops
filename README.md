# MLOps — Rolling Mean Signal Generator

A minimal MLOps-style batch job that computes a rolling mean on OHLCV close prices and generates a binary trading signal.

## Features
- **Reproducible**: Deterministic via YAML config + random seed
- **Observable**: Structured JSON metrics + detailed log file
- **Deployment-ready**: Dockerized, single-command run

## Local Setup

```bash
# Install dependencies
pip install -r requirements.txt

# Run the pipeline
python run.py --input data.csv --config config.yaml --output metrics.json --log-file run.log
```

## Docker

```bash
# Build
docker build -t mlops-task .

# Run
docker run --rm mlops-task
```

To extract output files from Docker:
```bash
docker run --rm -v $(pwd)/output:/app/output mlops-task \
  python run.py --input data.csv --config config.yaml \
  --output output/metrics.json --log-file output/run.log
```

## Example Output

### metrics.json
```json
{
  "version": "v1",
  "rows_processed": 9996,
  "metric": "signal_rate",
  "value": 0.499,
  "latency_ms": 127,
  "seed": 42,
  "status": "success"
}
```

> **Note**: `rows_processed` is 9996 (not 10000) because the first `window - 1 = 4` rows
> have no valid rolling mean and are excluded from signal computation.

## Configuration

| Key | Type | Description |
|-----|------|-------------|
| `seed` | int | Random seed for reproducibility |
| `window` | int | Rolling mean window size |
| `version` | string | Pipeline version tag |

## Signal Logic
- `signal = 1` if `close > rolling_mean`
- `signal = 0` otherwise
- Rows where rolling mean is undefined (first `window-1` rows) are excluded