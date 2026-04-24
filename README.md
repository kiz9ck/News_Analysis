```
      ::::::::  :::::::::  :::   ::: ::::::::: ::::::::::: :::::::: 
    :+:    :+: :+:    :+: :+:   :+: :+:    :+:    :+:    :+:    :+: 
   +:+        +:+    +:+  +:+ +:+  +:+    +:+    +:+    +:+    +:+  
  +#+        +#++:++#:    +#++:   +#++:++#+     +#+    +#+    +:+   
 +#+        +#+    +#+    +#+    +#+           +#+    +#+    +#+    
#+#    #+# #+#    #+#    #+#    #+#           #+#    #+#    #+#     
########  ###    ###    ###    ###           ###     ########       
      ::::    ::: :::::::::: :::       :::  ::::::::                
     :+:+:   :+: :+:        :+:       :+: :+:    :+:                
    :+:+:+  +:+ +:+        +:+       +:+ +:+                        
   +#+ +:+ +#+ +#++:++#   +#+  +:+  +#+ +#++:++#++                  
  +#+  +#+#+# +#+        +#+ +#+#+ +#+        +#+                   
 #+#   #+#+# #+#         #+#+# #+#+#  #+#    #+#                    
###    #### ##########   ###   ###    ######## 
```                     

# Crypto News Pipeline & LLM Analyzer

An automated data engineering pipeline that aggregates cryptocurrency news from multiple RSS feeds, parses the content, and utilizes Google's Gemini LLM (Vertex AI) to generate structured sentiment analysis. The system is designed to run asynchronously on Google Cloud Functions, storing historical data and market sentiment scores in BigQuery.

## Architecture & Tech Stack

* **Language:** Python 3.14
* **Compute:** Google Cloud Functions (`functions-framework`)
* **Storage:** Google BigQuery / Google Cloud Storage
* **AI/ML:** Google Vertex AI (Gemini 2.5 Flash)
* **CI/CD:** GitHub Actions

## Key Features

* **High-Concurrency Parsing:** Asynchronous feed aggregation and full-text extraction using `httpx` and `asyncio`.
* **Smart Deduplication:** SHA-256 hash-based identification prevents redundant database entries.
* **LLM Sentiment Scoring:** Evaluates news impact (-1.0 to +1.0), time horizons, and confidence levels using strict JSON schemas to prevent LLM hallucinations.
* **Market Context Integration:** Cross-references article publication times with historical price data to determine if the news is already priced into the market.
* **Resilient Error Handling:** Implements exponential backoff for external API rate limits and maintains a robust error-tracking table.

## Pipeline Flow

1.  **Ingestion:** Fetches feeds from 30+ crypto and macroeconomic news sources.
2.  **Enrichment:** Downloads full article text if the RSS summary is insufficient.
3.  **Storage (Raw):** Deduplicates and saves raw content to `raw_news` BigQuery table.
4.  **Analysis:** Triggers Vertex AI to evaluate sentiment based on current market prices.
5.  **Storage (Processed):** Saves structured JSON analysis to `analysis_results` BigQuery table.

## Configuration (Environment Variables)

The application relies on the following environment variables for configuration. No hardcoded secrets are included in the source code.

| Variable | Description | Default / Example |
| :--- | :--- | :--- |
| `PROJECT_ID` | GCP Project ID | `your-project-id` |
| `REGION` | GCP compute region | `europe-west1` |
| `BQ_NEWS_TABLE` | BigQuery table for raw news | `project.dataset.raw_news` |
| `BQ_RESULTS_TABLE` | BigQuery table for LLM analysis | `project.dataset.analysis_results` |
| `BATCH_SIZE` | Max rows processed per LLM run | `50` |
| `LLM_CONCURRENCY` | Max simultaneous requests to Vertex AI | `10` |

## Note on CI/CD and Deployment Security

This repository includes a fully configured Continuous Integration and Continuous Deployment (CI/CD) pipeline located in `.github/workflows/deploy.yml`. 

**Security Notice:** For the purpose of this public portfolio repository, the final deployment step to Google Cloud Platform is simulated. Real deployment requires GCP Service Account keys (`GCP_SA_KEY`), which are intentionally omitted from this repository's GitHub Secrets to protect cloud infrastructure and data integrity. The pipeline successfully demonstrates code checkout, environment setup, and automated flake8 linting.

## Legal & Terms of Use

This codebase is provided for educational and portfolio demonstration purposes. Web scraping and feed aggregation functionality should be used in compliance with the Terms of Service of the respective source websites.