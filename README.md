# 2020 General Election Mail Ballot Analysis

This project analyzes mail ballot requests for the 2020 General Election using data from an API. It covers the end-to-end pipeline, from pulling data, preprocessing to answering analytical questions with visualizations.

---

## Introduction

The dataset provides insights into mail ballot requests in Pennsylvania for the 2020 General Election. The project focuses on understanding trends, relationships between demographics and voting behavior, and district-level statistics.

---

## Data Pipeline

1. **Data ingestion:**
   - Fetch data in chunks using the API and save to a CSV file for local processing.  
2. **Data validation:**
   - Filters out invalid data rows containing null values in any column.  
3. **Data transformation:**
   - Transforms important fields for better understanding.  
4. **Analysis & Visualization:**
   - Examine the relationship between age, party affiliation, and vote-by-mail requests.
   - Measure application latency for each legislative district.
   - Identify districts with the highest frequency of ballot requests.
   - Visualize party application counts by county.
