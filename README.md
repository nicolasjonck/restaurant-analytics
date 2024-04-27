# TLDR
Data engineering project to help restaurants and waiters understand through dedicated dashboards:
- restaurants: sales statistics, key sales drivers and sales based on weather conditions
- waiters: performance based on sales, customers served and tips


**Tech stack**: Python, Docker, dbt, Airflow, BigQuery, Looker

# Pain
- **Restaurants** are making decisions based on gut feeling, potentially **missing out on opportunities** to make their business more profitable.​
- **Waiters**  can only rely on **subjective performance feedback** from managers.

# Solution
## Restaurant sales report  
This Looker dashboard enables restaurants to better understand sales, trends, and predict future sales

Some valuable insights:
- 📈 Sales trend over time
- 🕙 Sales based on time of day / day of week
- 🚀 Top-selling drinks and foods
- ☀️ Weather filter to understand which drinks and food sell better depending on temperature and rainfall/snow

![image](https://github.com/nicolasjonck/restaurant-analytics/assets/30000902/d969612d-7b71-4dd0-8140-01d2778d3c4b)


## Waiter performance report  
This dashboard enables waiters to understand their sales, number of customers served, and tips

![image](https://github.com/nicolasjonck/restaurant-data-backend/assets/30000902/0993ab2a-684d-4d1c-bb3e-a6551fc29edf)



# Architecture
## Data sources:
- Sales data: real-world point of sales data (Tiller) of 20 restaurants in Paris over a period of 7 years (2015-2021)
- Weather data: OpenWeatherMap data

## Ingestion:
- One-off data upload to BigQuery with Python
- Daily data fetching from OpenWeatherMap API and upload to BigQuery with Python

## Transformation:
Since this was real world data from restaurants with different semantics, data transformations concerned 3 categories:
- Data cleaning: remove all non-relevant order items (e.g. deposits, promotions),
- Consolidation: unify category types from over 400 to 8 different order categories
- Value creation: selecting only relevant columns and adding columns allowing for valuable insights (e.g. extraction of tips, dinner duration, dine-in vs take-away, mapping of weather to orders)

All data was ingested in the bronze zone, data was then cleaned and stored in the silver layer, before being aggregated to 2 final tables for exposure in the dashboard (restaurant sales and waiter performance).

## Hosting:
Docker compose to ensure availability and scalability

![image](https://github.com/nicolasjonck/restaurant-data-backend/assets/30000902/065b5844-e735-4853-bc59-bd226d3dc981)




