-- SQL Project: Data Cleaning Process

-- Source: Layoffs Dataset from Kaggle (https://www.kaggle.com/datasets/swaptr/layoffs-2022)

-- Step 1: View the original dataset
SELECT * 
FROM world_layoffs.layoffs;

-- Step 2: Create a staging table to safely clean and manipulate the data.
-- This helps preserve the original dataset in case I make mistakes or need to reference it later.
CREATE TABLE world_layoffs.layoffs_staging 
LIKE world_layoffs.layoffs;

-- Copy all data from the original table into the staging table
INSERT layoffs_staging 
SELECT * FROM world_layoffs.layoffs;

-- General Data Cleaning Steps I’ll follow:
-- 1. Identify and remove duplicate records
-- 2. Standardize the data format and correct errors
-- 3. Handle missing (NULL) or blank values appropriately
-- 4. Remove unnecessary columns or rows that add no value

-- ========== Step 1: Remove Duplicate Records ==========

-- First, check for any duplicates in the staging table
SELECT *
FROM world_layoffs.layoffs_staging;

-- Generate row numbers for potential duplicates based on key columns
SELECT company, industry, total_laid_off, `date`,
       ROW_NUMBER() OVER (
           PARTITION BY company, industry, total_laid_off, `date`
       ) AS row_num
FROM world_layoffs.layoffs_staging;

-- Identify exact duplicates by selecting records where the row number is greater than 1
SELECT *
FROM (
    SELECT company, industry, total_laid_off, `date`,
           ROW_NUMBER() OVER (
               PARTITION BY company, industry, total_laid_off, `date`
           ) AS row_num
    FROM world_layoffs.layoffs_staging
) duplicates
WHERE row_num > 1;

-- Manually inspect rows from specific companies like 'Oda' to verify if they are actual duplicates
SELECT *
FROM world_layoffs.layoffs_staging
WHERE company = 'Oda';

-- Now identify the real duplicates using more columns to ensure accuracy
SELECT *
FROM (
    SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions,
           ROW_NUMBER() OVER (
               PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
           ) AS row_num
    FROM world_layoffs.layoffs_staging
) duplicates
WHERE row_num > 1;

-- Option 1: Use a Common Table Expression (CTE) to delete duplicates directly from the staging table
WITH DELETE_CTE AS (
    SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions,
           ROW_NUMBER() OVER (
               PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
           ) AS row_num
    FROM world_layoffs.layoffs_staging
)
DELETE FROM world_layoffs.layoffs_staging
WHERE (company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, row_num) IN (
    SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, row_num
    FROM DELETE_CTE
) AND row_num > 1;

-- Option 2: Add a new column for row numbers, then delete rows where row number is 2 or higher

ALTER TABLE world_layoffs.layoffs_staging ADD row_num INT;

-- View the table after adding the new column
SELECT * FROM world_layoffs.layoffs_staging;

-- Create a new staging table (layoffs_staging2) and include the row numbers
CREATE TABLE world_layoffs.layoffs_staging2 (
    company TEXT,
    location TEXT,
    industry TEXT,
    total_laid_off INT,
    percentage_laid_off TEXT,
    date TEXT,
    stage TEXT,
    country TEXT,
    funds_raised_millions INT,
    row_num INT
);

-- Insert records into the new table while calculating row numbers
INSERT INTO world_layoffs.layoffs_staging2
SELECT company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions,
       ROW_NUMBER() OVER (
           PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions
       ) AS row_num
FROM world_layoffs.layoffs_staging;

-- Now delete all duplicate rows where row_num is greater than or equal to 2
DELETE FROM world_layoffs.layoffs_staging2
WHERE row_num >= 2;

-- ========== Step 2: Standardize and Correct Data ==========

-- View distinct values in the 'industry' column to find inconsistencies
SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY industry;

-- View rows with NULL or blank industry values
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL OR industry = ''
ORDER BY industry;

-- Replace empty string values in 'industry' with NULL for consistency
UPDATE world_layoffs.layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- Automatically populate missing 'industry' values by using entries from the same company that do have an industry listed
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
  ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL AND t2.industry IS NOT NULL;

-- Standardize variations of "Crypto" to a single consistent term
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry IN ('Crypto Currency', 'CryptoCurrency');

-- Check for inconsistencies in the 'country' column, such as trailing periods
SELECT DISTINCT country
FROM world_layoffs.layoffs_staging2
ORDER BY country;

-- Remove any trailing periods from country names
UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);

-- Fix and convert the 'date' column from string to proper DATE format
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- Change the column type of 'date' from TEXT to DATE
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- ========== Step 3: Review Null Values ==========

-- Check for NULL values in key numeric columns
-- These NULLs seem expected and useful for analysis later, so I’ll leave them as is
-- Having them as NULL makes it easier to use aggregate functions during analysis

-- ========== Step 4: Remove Irrelevant Rows and Columns ==========

-- View rows where total_laid_off is NULL
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL;

-- Further filter rows where both total_laid_off and percentage_laid_off are NULL
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

-- These rows provide no useful data, so I’ll delete them
DELETE FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

-- Drop the row_num column now that it’s no longer needed
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- Final view of cleaned data
SELECT * 
FROM world_layoffs.layoffs_staging2;
