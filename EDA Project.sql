-- EXPLORATORY DATA ANALYSIS (EDA)
-- This is the exploratory data analysis I performed to identify trends, patterns, and any anomalies such as outliers.
-- I began without a specific hypothesis and explored the dataset freely to uncover interesting insights.

SELECT * 
FROM world_layoffs.layoffs_staging2;

-- BASIC QUERIES

-- Find the maximum number of layoffs in a single record
SELECT MAX(total_laid_off) AS max_total_laid_off
FROM world_layoffs.layoffs_staging2;

-- Analyze the range of layoff percentages
SELECT MAX(percentage_laid_off) AS max_percentage, MIN(percentage_laid_off) AS min_percentage
FROM world_layoffs.layoffs_staging2
WHERE percentage_laid_off IS NOT NULL;

-- Identify companies that laid off 100% of their workforce
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE percentage_laid_off = 1;

-- Review how much funding those companies raised
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

-- GROUPED ANALYSIS -------------------------------------------------------------------------------

-- Companies with the largest single layoff events
SELECT company, total_laid_off
FROM world_layoffs.layoffs_staging
ORDER BY total_laid_off DESC
LIMIT 5;

-- Companies with the highest total layoffs overall
SELECT company, SUM(total_laid_off) AS total_laid_off
FROM world_layoffs.layoffs_staging2
GROUP BY company
ORDER BY total_laid_off DESC
LIMIT 10;

-- Locations with the most layoffs
SELECT location, SUM(total_laid_off) AS total_laid_off
FROM world_layoffs.layoffs_staging2
GROUP BY location
ORDER BY total_laid_off DESC
LIMIT 10;

-- Total layoffs by country
SELECT country, SUM(total_laid_off) AS total_laid_off
FROM world_layoffs.layoffs_staging2
GROUP BY country
ORDER BY total_laid_off DESC;

-- Total layoffs by year
SELECT YEAR(date) AS year, SUM(total_laid_off) AS total_laid_off
FROM world_layoffs.layoffs_staging2
GROUP BY YEAR(date)
ORDER BY year ASC;

-- Total layoffs by industry
SELECT industry, SUM(total_laid_off) AS total_laid_off
FROM world_layoffs.layoffs_staging2
GROUP BY industry
ORDER BY total_laid_off DESC;

-- Total layoffs by funding stage
SELECT stage, SUM(total_laid_off) AS total_laid_off
FROM world_layoffs.layoffs_staging2
GROUP BY stage
ORDER BY total_laid_off DESC;

-- ADVANCED QUERIES -------------------------------------------------------------------------------

-- Top 3 companies by layoffs per year
WITH Company_Year AS (
  SELECT 
    company, 
    YEAR(date) AS year, 
    SUM(total_laid_off) AS total_laid_off
  FROM layoffs_staging2
  GROUP BY company, YEAR(date)
),
Company_Year_Rank AS (
  SELECT 
    company, 
    year, 
    total_laid_off, 
    DENSE_RANK() OVER (PARTITION BY year ORDER BY total_laid_off DESC) AS rank
  FROM Company_Year
)
SELECT 
  company, 
  year, 
  total_laid_off, 
  rank
FROM Company_Year_Rank
WHERE rank <= 3
  AND year IS NOT NULL
ORDER BY year ASC, total_laid_off DESC;

-- Monthly layoffs trend
SELECT 
  SUBSTRING(date, 1, 7) AS month, 
  SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY month
ORDER BY month ASC;

-- Cumulative rolling layoffs by month
WITH Monthly_Layoffs AS (
  SELECT 
    SUBSTRING(date, 1, 7) AS month, 
    SUM(total_laid_off) AS total_laid_off
  FROM layoffs_staging2
  GROUP BY month
)
SELECT 
  month, 
  SUM(total_laid_off) OVER (ORDER BY month ASC) AS rolling_total_layoffs
FROM Monthly_Layoffs
ORDER BY month ASC;
