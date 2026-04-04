# Interview Prep — Product Data Science & Analytics Engineering

## Purpose
Interview preparation covering SQL, product analytics, A/B testing, statistics, data modeling, and analytics engineering concepts.

## Structure
```
/sql
  /window-functions
  /aggregations
  /joins
  /subqueries
  /optimization
/product-analytics
  /metric-design
  /ab-testing
  /case-studies
  /funnel-analysis
/statistics
  /probability
  /hypothesis-testing
  /distributions
  /regression
/analytics-engineering
  /dbt
  /data-modeling
  /warehousing
  /pipelines
/python
  /pandas
  /numpy
  /visualization
  /ml-basics
/behavioral
```

## SQL Conventions
- One file per problem/concept
- Include: source (e.g. StrataScratch, LeetCode, DataLemur), difficulty, topic tags
- Show the question as a comment block, then the query, then explanation

### SQL File Template
```sql
-- Problem: <name> — <source>
-- Difficulty: Easy / Medium / Hard
-- Topics: Window Functions, CTEs
--
-- Question:
-- <paste question here>
--
-- Approach:
-- <1-2 lines on how you solved it>

WITH cte AS (
  ...
)
SELECT ...
FROM cte;

-- Notes:
-- <any gotchas, alternative approaches, or follow-up variants>
```

## Product Analytics & Case Studies
- Store as Markdown
- Structure each case: Problem → Metrics → Hypothesis → Analysis → Recommendation
- Topics: funnel analysis, retention, engagement, North Star metrics, guardrail metrics

## A/B Testing
- Cover: experiment design, sample size, power, p-values, confidence intervals, novelty effect, network effects, Bayesian vs frequentist
- Python scripts to calculate sample size and run significance tests

## Statistics
- Python (numpy/scipy/statsmodels) for working through concepts
- File per topic with explanation + worked example

## Analytics Engineering
- dbt: models, tests, documentation, materialization strategies
- Data modeling: star schema, slowly changing dimensions, grain
- Warehousing: BigQuery, Snowflake, Redshift concepts

## Python Conventions
- Use pandas, numpy, matplotlib/seaborn, scikit-learn
- Each file: problem/concept at top, clean readable code, output shown in comments

## Progress Tracker
Maintain `PROGRESS.md` with topic checklists per category and notes on weak areas.

## Commit Style
- `practice: sql — window functions`
- `notes: ab-testing — sample size calculation`
- `case: <company or scenario>`

## GitHub
- Public repo for portfolio visibility
- README with topic checklist and study timeline
