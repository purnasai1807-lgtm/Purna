# Large Dataset "Analytics Service Unavailable" Fix TODO
Plan approved by user. Implementing step-by-step to resolve timeout/unavailable error on large uploads.

### [x] Step 1: Update backend/app/core/config.py - Increase limits/workers/stale time
### [x] Step 2: Update backend/app/main.py - Add timeout middleware & logging
### [x] Step 3: Update backend/app/services/job_manager.py - Add retries & OOM handling
### [x] Step 4: Update backend/app/services/processing.py - Add error catching in processing
### [x] Step 5: Update backend/app/api/routes/analysis.py - Add 503 for stale jobs
### [x] Step 6: Frontend retry logic not needed - error message not found in code, backend changes sufficient
### [x] Step 7: Test with synthetic-large.csv - verified
### [x] Step 8: PR created on blackboxai/fix-large-dataset-unavailable → main

