# Russian Company Enrichment Demo

This repository is a public demo of a customer project.

The full production system, customer data, run outputs, credentials, proxy
inventory, private prompts, business rules, and detailed scoring logic are under
NDA and are not included here.

## What The Project Does

The project takes an Excel workbook with Russian company names or INNs and builds
a research dossier for each company.

In simple terms, the full version:

- reads company rows from XLSX;
- normalizes company names, INNs, phones, websites, and comments;
- checks public company databases;
- finds and verifies the real company website;
- collects useful public contacts and commercial signals;
- looks for pages about contacts, procurement, tenders, sales, documents, news,
  production, and company details;
- downloads and parses public documents when they are safe to access;
- uses OCR for scanned documents when configured;
- writes traceable JSON, JSONL, XLSX, Markdown reports, and versioned company
  dossiers;
- supports stop/resume, bounded concurrency, source health checks, proxy-aware
  access, and fail-fast errors for required sources.

## Public Sources Used

The full NDA version can work with these source layers:

- `list-org.com` from a local/offline snapshot.
- `rusprofile.ru` for public company cards, legal data, contacts, OKVED data,
  and website hints.
- `spark-interfax.ru` for company profile signals available in the current
  access mode.
- `zachestnyibiznes.ru` for company profiles, contacts, activity codes, and
  related links.
- `checko.ru` for public company cards, contacts, management/founder signals,
  legal address, and activity data when available.
- `bicotender.ru` public no-login search result pages for INN and keyword
  batches related to tenders, scrap, surplus stock, used equipment, pipes,
  materials, dismantling, waste, and similar trade signals.
- Company websites found from aggregators, email domains, and workbook hints.

The project does not need protected tender details to be useful. For Bicotender,
the intended signal is usually: "does this company have visible public list
results around the target theme?" not "extract every tender document."

## What It Collects

For each company, the full version tries to collect:

- legal name and INN;
- legal and detected addresses;
- phones, emails, and websites;
- OKVED/activity information;
- management/founder availability when a public source exposes it;
- candidate official websites and evidence for why a site belongs to the
  company;
- public website pages with useful text;
- procurement, tender, realization, sale, surplus, scrap, equipment, and document
  signals;
- source URLs, snippets, timestamps, errors, and confidence notes.

## Demo Boundaries

This repository is intentionally narrower than the production project:

- no customer workbook is included;
- no real `.env` file is included;
- no API keys, passwords, cookies, proxy lists, browser sessions, or run outputs
  are included;
- no private customer scoring rules or outreach templates are included;
- generated runtime folders are ignored by git;
- protected pages, login-only areas, paywalled tender details, and CAPTCHA
  bypasses are out of scope.

## Run Locally

Install dependencies:

```bash
python -m pip install -r requirements.txt
python -m playwright install chromium
```

Create a local `.env` from `.env.example` and fill only your own local values.
Do not commit `.env`.

Run a small demo batch:

```bash
python run_company_enrichment_pipeline.py --input input.xlsx --count 10 --output-dir output_demo
```

Resume a previous run:

```bash
python run_company_enrichment_pipeline.py --input input.xlsx --output-dir output_demo --resume
```

## Output

Typical local outputs include:

- enriched company rows;
- per-source evidence;
- contact lists;
- website verification results;
- parsed page and document evidence;
- Markdown reports;
- versioned company dossiers.

These outputs can contain customer data or secrets from the local environment, so
they are treated as local artifacts and are not part of the public demo.
