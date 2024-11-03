<!-- markdownlint-disable MD026 -->
# Welcome to Nair-DAH! Let's BALL!!!

## Quickstart

### Authentication

Follow instructions from [API-NBA Documentation](https://api-sports.io/documentation/nba/v2). Store your API Key in a `.env` file.

```shell
API_KEY=<YOUR_API_KEY>
```

### Command

```bash
pip install -r requirements.txt
python main.py --ingest
```

Subsequent executions can omit the `--ingest` flag.

```bash
python main.py
```
