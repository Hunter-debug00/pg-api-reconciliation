# Reproducible Benchmark Results

#### 1. Correctness & Revenue Distortion

| Strategy                                   |   Ghosts |   Drift |   Backdated |   Seq Race |   Replica Lag |   Total False Del | Net MRR Diff   | Gross MRR Diff   |
|--------------------------------------------|----------|---------|-------------|------------|---------------|-------------------|----------------|------------------|
| 0. Incremental tombstone sync              |        2 |       0 |           0 |          0 |             0 |                 0 | $100.00        | $100.00          |
| 1. Naive OFFSET anti-join                  |        1 |       1 |           1 |          1 |             1 |                 4 | $200.00        | $300.00          |
| 2a. Keyset with source timestamp watermark |        1 |       0 |           1 |          0 |             1 |                 2 | $100.00        | $200.00          |
| 2b. Keyset with sequence watermark         |        1 |       0 |           0 |          1 |             1 |                 2 | $50.00         | $150.00          |
| 2c. Transactional epoch barrier            |        1 |       0 |           0 |          0 |             1 |                 1 | $0.00          | $100.00          |
| 3. Two-phase quarantine (streak >= 2)      |        1 |       0 |           0 |          0 |             0 |                 0 | $50.00         | $50.00           |
| 4. Consistent snapshot pagination          |        2 |       0 |           0 |          0 |             1 |                 1 | $50.00         | $150.00          |

#### 2. Database & API Execution Cost

| Strategy                                   |   API Reqs | Payload   | Pre-Scan DB   | Post-Scan DB   | Total DB (Med)   | Total DB (P95)   | DELETE Plan   | Shared Hits   |
|--------------------------------------------|------------|-----------|---------------|----------------|------------------|------------------|---------------|---------------|
| 0. Incremental tombstone sync              |          1 | 0.8 KB    | 0.00 ms       | 2.74 ms        | 2.74 ms          | 3.74 ms          | 0.00 ms       | N/A           |
| 1. Naive OFFSET anti-join                  |         50 | 29.4 KB   | 0.00 ms       | 64.27 ms       | 64.27 ms         | 102.08 ms        | 54.79 ms      | 78            |
| 2a. Keyset with source timestamp watermark |         50 | 30.5 KB   | 3.69 ms       | 47.49 ms       | 51.33 ms         | 60.02 ms         | 10.04 ms      | 15,073        |
| 2b. Keyset with sequence watermark         |         50 | 30.5 KB   | 3.69 ms       | 47.70 ms       | 51.41 ms         | 83.46 ms         | 10.86 ms      | 15,089        |
| 2c. Transactional epoch barrier            |         50 | 30.5 KB   | 5.25 ms       | 47.22 ms       | 53.24 ms         | 89.07 ms         | 9.78 ms       | 15,069        |
| 3. Two-phase quarantine (streak >= 2)      |        100 | 61.0 KB   | 6.79 ms       | 100.51 ms      | 107.29 ms        | 130.03 ms        | 0.09 ms       | 6             |
| 4. Consistent snapshot pagination          |         51 | 30.5 KB   | 5.67 ms       | 46.35 ms       | 53.91 ms         | 88.90 ms         | 9.75 ms       | 15,069        |

---

### Consolidated 17-Column Metric View

| Strategy                                   |   Ghosts |   Drift Del |   Backdated Del |   Seq Race Del |   Lag Del |   False Del | Net MRR Diff   | Gross MRR Diff   |   API Reqs | Body Bytes   | Pre-Scan DB   | Post-Scan DB   | Total DB (Med)   | Total DB (P95)   | DEL Plan (Med)   | DEL Plan Root Hits   |
|--------------------------------------------|----------|-------------|-----------------|----------------|-----------|-------------|----------------|------------------|------------|--------------|---------------|----------------|------------------|------------------|------------------|----------------------|
| 0. Incremental tombstone sync              |        2 |           0 |               0 |              0 |         0 |           0 | $100.00        | $100.00          |          1 | 0.8 KB       | 0.00 ms       | 2.74 ms        | 2.74 ms          | 3.74 ms          | 0.00 ms          | N/A                  |
| 1. Naive OFFSET anti-join                  |        1 |           1 |               1 |              1 |         1 |           4 | $200.00        | $300.00          |         50 | 29.4 KB      | 0.00 ms       | 64.27 ms       | 64.27 ms         | 102.08 ms        | 54.79 ms         | 78                   |
| 2a. Keyset with source timestamp watermark |        1 |           0 |               1 |              0 |         1 |           2 | $100.00        | $200.00          |         50 | 30.5 KB      | 3.69 ms       | 47.49 ms       | 51.33 ms         | 60.02 ms         | 10.04 ms         | 15073                |
| 2b. Keyset with sequence watermark         |        1 |           0 |               0 |              1 |         1 |           2 | $50.00         | $150.00          |         50 | 30.5 KB      | 3.69 ms       | 47.70 ms       | 51.41 ms         | 83.46 ms         | 10.86 ms         | 15089                |
| 2c. Transactional epoch barrier            |        1 |           0 |               0 |              0 |         1 |           1 | $0.00          | $100.00          |         50 | 30.5 KB      | 5.25 ms       | 47.22 ms       | 53.24 ms         | 89.07 ms         | 9.78 ms          | 15069                |
| 3. Two-phase quarantine (streak >= 2)      |        1 |           0 |               0 |              0 |         0 |           0 | $50.00         | $50.00           |        100 | 61.0 KB      | 6.79 ms       | 100.51 ms      | 107.29 ms        | 130.03 ms        | 0.09 ms          | 6                    |
| 4. Consistent snapshot pagination          |        2 |           0 |               0 |              0 |         1 |           1 | $50.00         | $150.00          |         51 | 30.5 KB      | 5.67 ms       | 46.35 ms       | 53.91 ms         | 88.90 ms         | 9.75 ms          | 15069                |
