# Bulk Certificate Generator

FastAPI backend that creates visually formatted, one-page participation or achievement certificates from a single predefined ReportLab template. Jobs accept up to 200 recipients, run in the background, expose live progress, and continue processing when one recipient fails.

## Setup

Python 3.11+ is recommended.

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install fastapi "sqlalchemy>=2" reportlab uvicorn httpx pytest python-multipart
```

## Run the application

```powershell
python app.py serve
```

The API is available at `http://127.0.0.1:8000`; interactive documentation is at `/docs`.

Optional settings:

```powershell
$env:CERT_DB_URL = "sqlite:///./certificates.db"
$env:CERT_STORAGE_DIR = "./generated_certificates"
```

## Run tests

```powershell
python app.py test
```

The embedded suite tests validation, certificate rendering, progress, bulk sizes, individual failures, PDF/ZIP downloads, cross-job access control, and both CSV input formats.

## Verify end to end from a terminal

Start the server in Terminal 1:

```powershell
.\.venv\Scripts\python.exe app.py serve
```

In Terminal 2, submit a JSON request:

```powershell
$body = @{
    event_name = "Rahul Participation Workshop"
    certificate_type = "Participation"
    issuer_name = "SHUSMIT"
    issuer_title = "Program Director"
    issue_date = "2026-03-03"
    recipients = @(@{name = "Rahul"; email = "rahul@example.com"})
} | ConvertTo-Json -Depth 5
$job = Invoke-RestMethod http://127.0.0.1:8000/jobs -Method Post `
    -ContentType "application/json" -Body $body
$status = Invoke-RestMethod "http://127.0.0.1:8000$($job.status_url)"
$status | ConvertTo-Json -Depth 10
```

When the status is `completed`, download the first PDF:

```powershell
Invoke-WebRequest `
    -Uri "http://127.0.0.1:8000$($status.certificates[0].download_url)" `
    -OutFile ".\rahul-certificate.pdf"
Start-Process ".\rahul-certificate.pdf"
```

For CSV verification, create a separate file (do not redirect into `app.py`):

```powershell
@"
name
Rahul
Asha Sharma
Diego Martin
"@ | Set-Content -Encoding UTF8 ".\demo.csv"
```

Submit it and download the ZIP:

```powershell
$job = curl.exe -s -X POST http://127.0.0.1:8000/jobs/csv `
    -F "file=@demo.csv" `
    -F "event_name=Terminal CSV Demo" `
    -F "certificate_type=Participation" `
    -F "issuer_name=SHUSMIT" `
    -F "issuer_title=Program Director" `
    -F "issue_date=2026-03-03" | ConvertFrom-Json
$status = Invoke-RestMethod "http://127.0.0.1:8000$($job.status_url)"
Invoke-WebRequest `
    -Uri "http://127.0.0.1:8000$($status.download_all_url)" `
    -OutFile ".\demo-certificates.zip"
```

The live verification produced Rahul's PDF with HTTP `200`, and the three-row CSV job completed with three successful certificates and a ZIP response with HTTP `200`.

## Measured generation times

Measured on Windows with Python 3.13, SQLite, ReportLab, and the predefined template. Each measurement includes submitting the request and polling until the job reached `completed`; it excludes ZIP/PDF download time.

| Certificates | Result | Successful | Time |
|---:|---|---:|---:|
| 5 | completed | 5 | 0.355 seconds |
| 10 | completed | 10 | 0.496 seconds |
| 15 | completed | 15 | 0.734 seconds |
| 50 | completed | 50 | 2.193 seconds |
| 200 | completed | 200 | 10.281 seconds |

Actual times vary with CPU, disk, Python version, and concurrent workload. The API returns `202 Accepted` and processes jobs in the background; clients should poll the status URL rather than assume a fixed duration.

## Submit a JSON request

```powershell
curl.exe -X POST http://127.0.0.1:8000/jobs `
  -H "Content-Type: application/json" `
  -d "{\"event_name\":\"Advanced Strategic Innovation Workshop 2026\",\"certificate_type\":\"Participation\",\"issuer_name\":\"Jonathan Patterson\",\"issuer_title\":\"Program Director\",\"issue_date\":\"2026-03-03\",\"recipients\":[{\"name\":\"Harumi Kobayashi\",\"email\":\"harumi@example.com\"},{\"name\":\"Aarav Sharma\"}]}"
```

The response is `202 Accepted` and includes `job_id`, `status_url`, and `download_all_url`.

## Submit a CSV request

`POST /jobs/csv` accepts a multipart upload. The CSV must have a `name`, `full_name`, or `recipient_name` column. An optional `email`, `email_id`, `mail`, or `recipient_email` column is supported.

### Name-only CSV

```csv
name
Harumi Kobayashi
Aarav Sharma
```

```powershell
curl.exe -X POST http://127.0.0.1:8000/jobs/csv `
  -F "file=@names.csv" `
  -F "event_name=Advanced Strategic Innovation Workshop 2026" `
  -F "certificate_type=Participation" `
  -F "issuer_name=Jonathan Patterson" `
  -F "issuer_title=Program Director" `
  -F "issue_date=2026-03-03"
```

### CSV with email addresses

```csv
name,email
Harumi Kobayashi,harumi@example.com
Aarav Sharma,aarav@example.com
```

The email is stored with the recipient result for reference. This implementation only generates and retrieves certificates; email sending is intentionally out of scope.

Rows with blank names or invalid email addresses are recorded as failed recipients while valid rows continue.

## Check status and retrieve certificates

```powershell
curl.exe http://127.0.0.1:8000/jobs/{job_id}
curl.exe -o certificates.zip http://127.0.0.1:8000/jobs/{job_id}/download
curl.exe -o certificate.pdf http://127.0.0.1:8000/jobs/{job_id}/certificates/{certificate_id}
```

Job status includes `pending`, `processing`, `completed`, `completed_with_errors`, or `failed`, plus total, succeeded, failed, pending, progress percentage, and per-recipient download URLs.

## Function reference

- `create_app`: builds the FastAPI application, database tables, storage directory, lifecycle recovery, and routes.
- `create_job`: accepts a JSON generation request.
- `create_job_from_csv`: parses a name-only or name/email CSV and creates a generation job.
- `_create_job_from_recipients`: validates each recipient and queues the shared processing workflow.
- `process_job`: renders pending certificates independently and updates progress after every row.
- `render_certificate`: draws the fixed certificate design and writes an atomic PDF.
- `job_status`: returns live job progress and recipient results.
- `get_certificate`: returns one successful PDF.
- `download_all`: returns successful PDFs as a ZIP.

## Design decisions

- FastAPI `BackgroundTasks` returns `202` quickly and lets clients poll status. It is appropriate for this assignment; production deployments can replace it with Celery/RQ.
- SQLite is the default relational database, and SQLAlchemy models the one-to-many job/certificate relationship.
- Request-level errors reject the whole request with `422`; recipient-level errors are isolated and recorded in the job.
- PDFs use a single vector template with blue decorative waves, a gold seal, recipient name, event, certificate type, date, and issuer.
- Files use generated IDs rather than user input, preventing path traversal. A temporary PDF is atomically renamed only after rendering completes.
