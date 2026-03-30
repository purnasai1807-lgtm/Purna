# Performance & Upload Fix TODO
Approved plan for large dataset slowdown + upload errors. Tracking progress:

### [x] Step 1: Update config.py ✅ - Increase limits/memory
- max_upload_size_mb=500
- analytics_memory_limit_mb=8192
- analytics_sample_rows=20000

### [x] Step 2: Update storage.py ✅ - Graceful large/empty handling

### [x] Step 3: Update analysis.py ✅ - Better 413/UX

### [x] Step 4: Optimize processing.py ✅ - No change needed (efficient)

### [ ] Step 5: Optimize analytics.py - More sampling

### [ ] Step 6: Test with synthetic-large.csv
- Time upload/process
- No errors

### [ ] Step 7: Complete!

