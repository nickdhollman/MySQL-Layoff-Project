#### Data Cleaning #### 
USE world_layoffs;
SELECT *
FROM layoffs;

####  Create copy of original table, you don't want to make any edits on raw data table #### 
CREATE TABLE layoffs_staging
LIKE layoffs;
####  insert data in new table from original #### 
INSERT layoffs_staging
SELECT * 
FROM layoffs;

#### 1. Remove Duplicates #### 
SELECT *
FROM layoffs_staging;
-- Create row number to identify duplicates by company, industry, total_laid_off, 
-- percentage_laid_off, and date of lay off 
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, stage, country, funds_raised_millions, `date`) AS row_num
FROM layoffs_staging;
-- A row number of 1 = no duplicate, a row number of 2 or above is indicative of duplication
-- Create CTE to identify duplicates
-- A CTE (Common Table Expression) in MySQL is a temporary result set that you can 
-- reference within a SELECT, INSERT, UPDATE, or DELETE statement. It helps make complex 
-- queries more readable and modular. It is temporary instead of VIEWS that are stored 
-- w/n the database 
WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, stage, country, funds_raised_millions, `date`) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte 
WHERE row_num > 1;
-- Confirm duplication worked
SELECT *
FROM layoffs_staging
WHERE company = 'Hibob';
-- Remove duplicates (row_number > 1) by creating a new table with row_number,
-- You can't make deletions/updates on cte 
-- to get below right click on table - click copy to keyboard - CREATE table
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
-- insert data into table
INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, stage, country, funds_raised_millions, `date`) AS row_num
FROM layoffs_staging;
-- layoffs_staging2 now has all data from layoffs_staging with row_num column
SELECT *
FROM layoffs_staging2
WHERE row_num > 1;
-- now delete data selected above
DELETE
FROM layoffs_staging2
WHERE row_num > 1;
-- verify deletion occurred
SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

####  2. Standardize the Data -- time stamp 17:37 in Data Cleaning in MySQL Alex Analyst video #### 
-- this is the process of finding issues in the data and fixing it 
### STANDARDIZE COMPANY ###
SELECT company, (TRIM(company)) -- TRIM takes white space of the text
FROM layoffs_staging2;
-- this displays the space before E Inc.
UPDATE layoffs_staging2
SET company = TRIM(company);
-- verify trim worked correctly
SELECT company, (TRIM(company)) 
FROM layoffs_staging2; 

### STANDARDIZE INDUSTRY ###
SELECT DISTINCT(industry)
FROM layoffs_staging2
ORDER BY 1; -- This is just ordering by the first column 
-- NULL and blank columns are problems that will need to be dealt with
-- Crypto, Crypto Currency, and CryptoCurrency all need to be the same thing for future analysis 
SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';
-- update 
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';
-- validate update worked correctly 
SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';
SELECT DISTINCT(industry)
FROM layoffs_staging2
ORDER BY 1;

### STANDARDIZE LOCATION ###
SELECT DISTINCT(location)
FROM layoffs_staging2
ORDER BY 1;
-- looks good, no standardization needed

### STANDARDIZE COUNTRY ###
SELECT DISTINCT(country)
FROM layoffs_staging2
ORDER BY 1;
-- period after United States needs fixed
SELECT DISTINCT(country)
FROM layoffs_staging2
WHERE country LIKE 'United States%';
-- could also do below to isolate with period 
SELECT *
FROM layoffs_staging2
WHERE country LIKE '%.';
-- TRAILING is telling MySQL 'coming at the end'
SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;
-- You can see United States. is now United States 
-- Now that we know the above works, we need to apply the update 
UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';
-- verify above code worked
SELECT DISTINCT(country)
FROM layoffs_staging2
ORDER BY 1;
-- worked 

### STANDARDIZE DATE - CHANGE TO DATE TYPE ###
-- STR_TO_DATE converts strings to dates, it requires the column argument (`date`) and the format you desire for the date ('%m/%d/%Y')
-- this is taking the string column of date and converting it to a date format with 4 digits for year - two digits for month - two digits for day 
-- there are different formats you can apply for date depending on how your original text column is formatted
SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y') 
FROM layoffs_staging2;
-- apply update 
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');
-- verify it worked
SELECT `date` 
FROM layoffs_staging2;
-- we now want to convert the date column to a date format
## NEVER DO THIS ON YOUR ORIGINAL TABLE!! ONLY STAGING/COPIES OF ORIGINAL TABLE 
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;
-- date column in layoffs_staging2 is now a date format 

####  3. Null Values or Blank Values #### 
-- we may want to remove these values later as they don't give us any useful information if we are interested in total/percent laid off
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
OR industry = '' ;

SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';
-- this tells us that Airbnb should be under the Travel industry, we need to update all values to this to do subgroup analysis later
-- we do not want to do that for just Airbnb, but all companies that are experiencing this problem (blank industry)
SELECT *
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;
-- Now we want to update
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company AND t1.location = t2.location
SET t1.industry = t2.industry -- stating to make t2 industry (the not blank industry) as the value for t1 industry (the value that is currently blank)
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;
-- The above did not work, we need to update the blank values to NULL values before updating industry 
UPDATE layoffs_staging2
SET industry = NULL 
WHERE industry = '';
-- re-run above select statement
SELECT *
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;
-- the values are now all NULL, not ''
-- re-run update, but you can delete the OR t1.industry = '' since all values are now NULL
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company AND t1.location = t2.location
SET t1.industry = t2.industry -- stating to make t2 industry (the not NULL industry) as the value for t1 industry (the value that is currently NULL)
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;
-- verify this worked by running query above 
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
OR industry = '' ;
-- Everything resolved except Bally's
SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally%';
-- this company only has one row with industry value of NULL, so we have nothing to update it on
-- We are not going to update total_laid_off, percentage_laid_off, or funds_raised_millions with NULL or blank values because we don't have any 
-- adequate information to update the NULL or blank values off of (we would need the total employees prior to layoff, or other information such as this)

####  4. Remove Any Columns / ROWS (you don't want to do this on the original table, you want to make a copy) #### 
-- IN THE NEXT PHASE OF THIS PROJECT WE ARE GOING TO BE INTERESTED IN TOTAL_LAID_OFF AND PERCENTAGE_LAID_OFF
-- BECAUSEE OF THIS, WE WANT TO DELETE THE FOLLOWING ROWS
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;
-- if you run SELECT query above you can verify these rows no longer exist 

SELECT *
FROM layoffs_staging2;

-- we no long need row_num column
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;
-- run SELECT statement above to verify this column is no longer in dataset







