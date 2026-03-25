# Auto Analytics AI

Auto Analytics AI is a production-oriented full-stack web application for automatic data analytics. Users can sign up, upload CSV or Excel files, enter data manually, and receive cleaned datasets, summary statistics, correlations, outlier detection, trends, charts, machine learning suggestions, saved history, PDF exports, and public share links.

## 1. Project Architecture

### Frontend
- `Next.js` app router project in [`frontend/`](./frontend)
- Public landing page, auth flow, protected dashboard, private report view, and public share page
- Responsive React UI with Plotly chart rendering and mobile-safe data tables

### Backend
- `FastAPI` API in [`backend/`](./backend)
- JWT authentication
- SQLAlchemy persistence for users and report history
- Analytics engine using `pandas`, `numpy`, `plotly`, and `scikit-learn`
- On-demand PDF export for reports

### Database
- PostgreSQL-ready schema for users and analysis reports
- SQL reference in [`docs/database-schema.sql`](./docs/database-schema.sql)

### Public deployment
- Frontend: Vercel
- Backend: Render or Railway
- Database: Neon PostgreSQL

## 2. Folder Structure

```text
.
|-- backend
|   |-- app
|   |   |-- api
|   |   |   `-- routes
|   |   |-- core
|   |   |-- db
|   |   |-- schemas
|   |   `-- services
|   |-- .env.example
|   `-- requirements.txt
|-- docs
|   `-- database-schema.sql
|-- frontend
|   |-- app
|   |-- components
|   |-- lib
|   |-- public
|   |   `-- sample-datasets
|   |-- .env.example
|   `-- package.json
|-- sample-data
|   `-- retail-performance.csv
|-- render.yaml
`-- README.md
```

## 3. Frontend Code

Key frontend files:
- [`frontend/app/page.tsx`](./frontend/app/page.tsx): landing page
- [`frontend/app/auth/page.tsx`](./frontend/app/auth/page.tsx): signup/login
- [`frontend/app/dashboard/page.tsx`](./frontend/app/dashboard/page.tsx): upload, manual entry, history
- [`frontend/app/analysis/[id]/page.tsx`](./frontend/app/analysis/[id]/page.tsx): authenticated report view
- [`frontend/app/share/[token]/page.tsx`](./frontend/app/share/[token]/page.tsx): public share page
- [`frontend/components/analysis/analysis-dashboard.tsx`](./frontend/components/analysis/analysis-dashboard.tsx): charts, insights, metrics, tables
- [`frontend/lib/api.ts`](./frontend/lib/api.ts): API integration layer

Frontend environment variables:

```bash
NEXT_PUBLIC_API_BASE_URL=http://localhost:8000/api/v1
LOCAL_BACKEND_API_URL=http://127.0.0.1:8000/api/v1
NEXT_PUBLIC_DIRECT_BACKEND_API_URL=http://127.0.0.1:8000/api/v1
```

## 4. Backend Code

Key backend files:
- [`backend/app/main.py`](./backend/app/main.py): FastAPI bootstrap and CORS
- [`backend/app/api/routes/auth.py`](./backend/app/api/routes/auth.py): signup, login, current user
- [`backend/app/api/routes/analysis.py`](./backend/app/api/routes/analysis.py): upload, manual entry, history, sharing, PDF export
- [`backend/app/services/analytics.py`](./backend/app/services/analytics.py): cleaning, profiling, trends, narrative insights
- [`backend/app/services/visualization.py`](./backend/app/services/visualization.py): chart generation
- [`backend/app/services/modeling.py`](./backend/app/services/modeling.py): regression, classification, clustering workflows
- [`backend/app/services/reporting.py`](./backend/app/services/reporting.py): PDF report builder

Backend environment variables:

```bash
APP_NAME=Auto Analytics AI API
APP_ENV=development
API_V1_PREFIX=/api/v1
SECRET_KEY=replace-with-a-long-random-secret
ACCESS_TOKEN_EXPIRE_MINUTES=1440
DATABASE_URL=postgresql+psycopg2://postgres:postgres@localhost:5432/auto_analytics_ai
CORS_ORIGINS=http://localhost:3000,https://your-vercel-app.vercel.app
REPORT_BASE_URL=http://localhost:3000
```

## 5. Database Schema

Two main tables are used:
- `users`: identity, email, hashed password, timestamps
- `analysis_reports`: saved results, report payload JSON, share token, ownership, timestamps

SQL reference:
- [`docs/database-schema.sql`](./docs/database-schema.sql)

## 6. Local Development

### Backend

```bash
cd backend
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env
uvicorn app.main:app --reload
```

### Frontend

```bash
cd frontend
npm install
copy .env.example .env.local
npm run dev
```

Frontend runs on `http://localhost:3000` and backend runs on `http://localhost:8000`.

### Keep the app running after closing VS Code on Windows

If you want the app to keep running after you close the editor, start it with the background launcher from the project root:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start-app.ps1
```

Helpful companion commands:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\status-app.ps1
powershell -ExecutionPolicy Bypass -File .\scripts\stop-app.ps1
```

What this does:
- builds the Next.js frontend
- starts the FastAPI backend and frontend as detached background processes
- writes logs and PID files to `.runtime/`

This keeps the app running after VS Code is closed, but it still depends on your computer staying on. For true 24/7 uptime, deploy the backend and frontend using the Render and Vercel steps below.

## 7. Production Deployment Steps

### Backend on Render
1. Push this repo to GitHub.
2. Create a Neon PostgreSQL database and copy the connection string.
3. In Render, create a new Web Service from the repo.
4. Set the service root directory to `backend`.
5. Use build command: `pip install -r requirements.txt`
6. Use start command: `uvicorn app.main:app --host 0.0.0.0 --port $PORT`
7. Add environment variables:
   - `DATABASE_URL`
   - `SECRET_KEY`
   - `CORS_ORIGINS`
   - `REPORT_BASE_URL`
8. Deploy and verify `https://your-backend-domain/health`

### Frontend on Vercel
1. Import the same repo into Vercel.
2. Set the root directory to `frontend`.
3. Add environment variable:
   - `NEXT_PUBLIC_API_BASE_URL=https://your-backend-domain/api/v1`
   - `NEXT_PUBLIC_DIRECT_BACKEND_API_URL=https://your-backend-domain/api/v1`
4. Deploy and verify the public site URL.

### Final production wiring
1. Update backend `CORS_ORIGINS` with your Vercel domain.
2. Update backend `REPORT_BASE_URL` with your public Vercel frontend URL.
3. Re-deploy the backend so share links point to the live frontend.

When deployed this way, the app is publicly accessible from anywhere on the internet, not limited to localhost or the same Wi-Fi network.

## 8. API Surface

Main endpoints:
- `POST /api/v1/auth/signup`
- `POST /api/v1/auth/login`
- `GET /api/v1/auth/me`
- `POST /api/v1/analysis/upload`
- `POST /api/v1/analysis/manual`
- `GET /api/v1/analysis/history`
- `GET /api/v1/analysis/reports/{report_id}`
- `POST /api/v1/analysis/reports/{report_id}/share`
- `GET /api/v1/analysis/shared/{share_token}`
- `GET /api/v1/analysis/reports/{report_id}/download-pdf`

## 9. Sample Dataset

Included sample data:
- [`sample-data/retail-performance.csv`](./sample-data/retail-performance.csv)
- [`frontend/public/sample-datasets/retail-performance.csv`](./frontend/public/sample-datasets/retail-performance.csv)

## 10. Error Handling and UX Notes

- Auth, upload, manual-entry, history, and report pages all surface API errors in the UI.
- Loading states are included for auth hydration, dashboard fetches, and report fetches.
- The interface is responsive for laptop and mobile screens.
- Public share pages do not require authentication.

## 11. Future Improvements

- Replace single-model baselines with full AutoML comparison pipelines
- Add background jobs for large-file processing
- Add role-based access control and team workspaces
- Add refresh tokens or cookie-based auth hardening
- Add schema-drift detection and richer PDF branding
- Add object storage for uploaded source files and generated assets
