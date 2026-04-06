# Class-Based Practice Problems — CodeSignal Format

Each level mirrors the exact CodeSignal format:
- A class file with method stubs + docstrings (you fill in the bodies)
- `basicTests.py` — visible tests (like what you see in CodeSignal)
- `advancedTests.py` — hidden-style tests (edge cases the grader checks)

## How to practice

```bash
# Run basic tests (do this first)
cd level1
python3 -m unittest basicTests -v

# Run advanced tests (do this after basic pass)
python3 -m unittest advancedTests -v

# Run all tests at once
python3 -m unittest discover -v
```

## Levels

| Level | File | What it tests |
|---|---|---|
| 1 | `level1/bikeProcessor.py` | Cleaning — invalid rows, duplicates, aggregations |
| 2 | `level2/bikeEnricher.py` | Joining — enrich trips with station data, route analysis |
| 3 | `level3_sql/problem.sql` | SQL — stored procedure, joins, aggregations |
| 4 | `level4/bikePipeline.py` | Pipeline — full ETL: ingest → validate → transform → report |

## Strategy reminder
1. Read every docstring carefully — that is your spec
2. Implement one method at a time
3. Run basic tests after each method
4. Always handle: empty input, all-invalid data, duplicates, None values
5. Don't optimize — just pass the tests
