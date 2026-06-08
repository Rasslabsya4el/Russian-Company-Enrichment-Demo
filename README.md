# Russian Company Enrichment Demo

Public demo of an NDA-covered production system for Russian company enrichment.

The full project turns a company identifier into a caller-ready lead profile: contacts, official website, business activity, tender/procurement signals, documents, evidence links, priority, and a short reason to call.

`INN` is a Russian Taxpayer ID - a unique company identifier, similar to a national business registration/tax number.

## NDA Boundary

This repository is a sanitized demo.

It keeps the architecture, parsing layers, runtime model, and tests. It does not include customer data, private prompts, credentials, proxy inventory, run outputs, final scoring rules, or customer-specific outreach templates.

## Product Goal

The system helps answer:

- Is this a real and relevant company?
- What does it actually do?
- Where is the official website?
- Which contacts are usable?
- Are there public procurement, surplus, stock, scrap, or material signals?
- Why should a caller spend time on this lead?
- Which evidence supports that decision?

## Why It Is Hard

Russian industrial company data is messy.

Public company profiles disagree. Contacts go stale. Websites are often old, broken, badly encoded, or partly JS-only. Useful signals hide inside weak wording, PDFs, spreadsheets, archives, scanned files, and tender/procurement pages.

The system is built for that reality: unreliable sources, source limits, sessions, proxy rotation, captcha handling, resume support, and evidence tracking.

## What Comes Out

The production output is a human-facing lead workbook.

It contains:

- clean company profile;
- address and geography;
- phone, email, and official website;
- readable activity codes;
- public tender/procurement signals;
- useful website and document evidence;
- priority score;
- short reason to call;
- clickable evidence links;
- separate evidence details when the main table would become too noisy.

The output is built for callers and sales teams, not for developers. It should not expose parser internals, batch names, raw logs, or confusing technical statuses.

## Three Pipeline Passes

### 1. Company Data Aggregation

Collects the base profile from public company-data aggregators and local snapshots.

Normalizes legal profile, addresses, contacts, websites, activity codes, and source links.

### 2. Tender And Procurement Signals

Checks public tender/procurement platforms for visible commercial signals:

- tenders;
- procurement;
- surplus stock;
- used equipment;
- scrap;
- materials;
- dismantling;
- warehouse leftovers;
- industrial sales.

The goal is not to access protected tender details. The goal is to detect useful public signals and attach evidence.

### 3. Official Site And Deep Evidence

Finds the real company website, verifies that it belongs to the company, then parses useful public pages and documents.

The deep pass can inspect contacts, about pages, production pages, procurement pages, sales pages, catalogs, public documents, and scanned files through OCR when configured.

## Async Runtime Architecture

The runtime is not a linear scraper.

Every scraping point and every pipeline stage reports into one central writer. Worker counts are configurable per stage.

```text
Input workbook
  INN / company name / hints
        |
        v
Runtime scheduler
  creates row work + stage work
        |
        v
Access layer
  sessions + proxy rotation + captcha handling
        |
        v
Parallel worker pools
  company data aggregators          \
  tender/procurement platforms       \
  website discovery                   \
  official-site verification           >  Central writer / state store
  deep website parsing                /   every stage sends events immediately
  documents + OCR                    /
  LLM review / scoring helpers      /
        |
        v
Live company state
  progress + evidence + errors + resume data
        |
        v
Final outputs
  caller workbook + evidence links + company dossier
```

The central writer receives structured events:

- source result ready;
- candidate website found;
- official-site decision made;
- deep parse finished;
- document parsed;
- company completed;
- runtime host event.

This gives the run a live state. If one source slows down, hits captcha, waits for proxy capacity, or fails, the rest of the system is still explainable and resumable.

## Runtime Controls

The full system supports:

- per-stage worker limits;
- source-specific lanes;
- proxy rotation for proxy-bound sources;
- session-bound serial access where needed;
- captcha detection and authorized captcha-handling hooks;
- host cooldowns;
- downstream backpressure;
- progress ledger;
- resume after stop/failure;
- source failure diagnostics.

This is the difference between a script and a batch system that can handle long, messy enrichment runs.

## Demo Scope

This public repo demonstrates:

- multi-source enrichment architecture;
- async fan-in into a central writer;
- configurable worker pools;
- proxy/session/captcha-aware source access;
- site verification;
- deep website parsing;
- document and OCR pipeline hooks;
- evidence-first output design;
- tests around runtime and parser failure modes.

Excluded from the public demo:

- customer workbooks;
- generated runtime outputs;
- real credentials;
- real proxy inventory;
- private prompts;
- final customer scoring logic;
- production outreach templates;
- confidential business rules.

## Portfolio Note

This README describes the full project at a high level. The repository is intentionally a demo because the real deployment and customer artifacts are under NDA.
