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
| 0. Incremental tombstone sync              |          1 | 0.8 KB    | 0.00 ms       | 2.77 ms        | 2.77 ms          | 4.51 ms          | 0.00 ms       | N/A           |
| 1. Naive OFFSET anti-join                  |         50 | 29.4 KB   | 0.00 ms       | 73.52 ms       | 73.52 ms         | 150.06 ms        | 61.97 ms      | 78            |
| 2a. Keyset with source timestamp watermark |         50 | 30.5 KB   | 3.78 ms       | 51.56 ms       | 56.97 ms         | 93.61 ms         | 10.42 ms      | 15,073        |
| 2b. Keyset with sequence watermark         |         50 | 30.5 KB   | 4.37 ms       | 52.12 ms       | 57.94 ms         | 127.56 ms        | 11.91 ms      | 15,089        |
| 2c. Transactional epoch barrier            |         50 | 30.5 KB   | 5.47 ms       | 51.69 ms       | 57.66 ms         | 88.03 ms         | 11.76 ms      | 15,069        |
| 3. Two-phase quarantine (streak >= 2)      |        100 | 61.0 KB   | 7.02 ms       | 105.35 ms      | 113.29 ms        | 145.72 ms        | 0.11 ms       | 6             |
| 4. Consistent snapshot pagination          |         51 | 30.5 KB   | 5.83 ms       | 50.64 ms       | 56.36 ms         | 102.42 ms        | 10.66 ms      | 15,069        |

---

### Consolidated 17-Column Metric View

| Strategy                                   |   Ghosts |   Drift Del |   Backdated Del |   Seq Race Del |   Lag Del |   False Del | Net MRR Diff   | Gross MRR Diff   |   API Reqs | Body Bytes   | Pre-Scan DB   | Post-Scan DB   | Total DB (Med)   | Total DB (P95)   | DEL Plan (Med)   | DEL Plan Root Hits   |
|--------------------------------------------|----------|-------------|-----------------|----------------|-----------|-------------|----------------|------------------|------------|--------------|---------------|----------------|------------------|------------------|------------------|----------------------|
| 0. Incremental tombstone sync              |        2 |           0 |               0 |              0 |         0 |           0 | $100.00        | $100.00          |          1 | 0.8 KB       | 0.00 ms       | 2.77 ms        | 2.77 ms          | 4.51 ms          | 0.00 ms          | N/A                  |
| 1. Naive OFFSET anti-join                  |        1 |           1 |               1 |              1 |         1 |           4 | $200.00        | $300.00          |         50 | 29.4 KB      | 0.00 ms       | 73.52 ms       | 73.52 ms         | 150.06 ms        | 61.97 ms         | 78                   |
| 2a. Keyset with source timestamp watermark |        1 |           0 |               1 |              0 |         1 |           2 | $100.00        | $200.00          |         50 | 30.5 KB      | 3.78 ms       | 51.56 ms       | 56.97 ms         | 93.61 ms         | 10.42 ms         | 15073                |
| 2b. Keyset with sequence watermark         |        1 |           0 |               0 |              1 |         1 |           2 | $50.00         | $150.00          |         50 | 30.5 KB      | 4.37 ms       | 52.12 ms       | 57.94 ms         | 127.56 ms        | 11.91 ms         | 15089                |
| 2c. Transactional epoch barrier            |        1 |           0 |               0 |              0 |         1 |           1 | $0.00          | $100.00          |         50 | 30.5 KB      | 5.47 ms       | 51.69 ms       | 57.66 ms         | 88.03 ms         | 11.76 ms         | 15069                |
| 3. Two-phase quarantine (streak >= 2)      |        1 |           0 |               0 |              0 |         0 |           0 | $50.00         | $50.00           |        100 | 61.0 KB      | 7.02 ms       | 105.35 ms      | 113.29 ms        | 145.72 ms        | 0.11 ms          | 6                    |
| 4. Consistent snapshot pagination          |        2 |           0 |               0 |              0 |         1 |           1 | $50.00         | $150.00          |         51 | 30.5 KB      | 5.83 ms       | 50.64 ms       | 56.36 ms         | 102.42 ms        | 10.66 ms         | 15069                |
