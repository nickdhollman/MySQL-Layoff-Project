#### EXPLORATORY DATA ANALYSIS ####
-- orient to dataset - this is the cleaned dataset from the Data_Cleaning_SQL syntax
SELECT * 
FROM layoffs_staging2;

SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging2;
-- 1 for percentag_laid_off is the entire company (100%) being laid off
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC;
-- funds raised by companies that were entirely shut down
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;
-- total laid off in descending order by company 
SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC; -- 2 is stating to order by the second requested column SUM(total_laid_off)
-- range of date
SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;
-- total laid off in descending order by industry 
SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;
-- total laid off in descending order by country
SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;
-- total laid off by individual date (descending)
SELECT `date`, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY `date`
ORDER BY 1 DESC;
-- total laid off by YEAR (descending)
SELECT YEAR(`date`), SUM(total_laid_off) -- YEAR() function is creating the YEAR from the date column 
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;
-- we are only in March of 2023, so we will have many more lay offs in 2023 if the trend continues compared to 2022
-- total laid off in descending order by stage 
SELECT stage, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;
-- pull out month column from data
SELECT `date`, SUBSTRING(`date`,6,2) AS `MONTH` -- SUBSTRING is pulling out the values starting at the 6th character of date for 2 characters 
FROM layoffs_staging2;
-- total laid off by month
SELECT SUBSTRING(`date`,6,2) AS `MONTH`, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY `MONTH`;
-- the problem with the above query is this does not give us the total by month and year, it just gives the total by month regardless of year
SELECT YEAR(`date`) AS YEAR, SUBSTRING(`date`,6,2) AS `MONTH`, SUM(total_laid_off)
FROM layoffs_staging2
WHERE YEAR(`date`) IS NOT NULL AND SUBSTRING(`date`,6,2) IS NOT NULL -- you must use this format and not call YEAR and MONTH in the WHERE clause
GROUP BY `MONTH`, YEAR
ORDER BY YEAR DESC, `MONTH`;
-- Alternatively you can produce the above in a simpler format with 
SELECT SUBSTRING(`date`,1,7) AS `YEAR_MONTH`, SUM(total_laid_off)
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `YEAR_MONTH`
ORDER BY 1 ASC;

-- Get rolling total of layoffs over time
WITH Rolling_Total AS
(
SELECT SUBSTRING(`date`,1,7) AS `YEAR_MONTH`, SUM(total_laid_off) AS total_off
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `YEAR_MONTH`
ORDER BY 1 ASC
)
SELECT `YEAR_MONTH`, total_off,
SUM(total_off) OVER(ORDER BY `YEAR_MONTH`) AS rolling_total
FROM Rolling_Total;

-- total laid off by company by descending order of total laid off
SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;
-- total laid off by company & year by descending order of total laid off
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC;
-- Create CTE from above because we additionally want to create ranking 
-- The Ranking column created is the rank for total number of lay offs per year
-- Uber Ranking 1 for 2020 = Uber laid off a total of 7,525 employees in 2020, which was th most by any company in our dataset
WITH Company_Year (Company, Years, Total_laid_off) AS -- We are creating this first CTE to create the Ranking off of 
(
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
), Company_Year_Rank AS -- We are creating this 2nd CTE to limit by Ranking off of 
(SELECT *, DENSE_RANK() OVER(PARTITION BY Years ORDER BY Total_laid_off DESC) AS Ranking
FROM Company_Year -- This is the first CTE
WHERE Years IS NOT NULL
)
SELECT *
FROM Company_Year_Rank
WHERE Ranking <= 5;

#### ADDITIONAL EDA QUERIES ###
-- Top 10 companies by Total Layoffs 
SELECT 
  company,
  SUM(total_laid_off) AS total_laid_off
FROM layoffs
GROUP BY company
ORDER BY total_laid_off DESC
LIMIT 10;
-- Layoff Percentage Distribution by Funding Stage
SELECT 
  stage,
  AVG(percentage_laid_off) AS avg_laid_off_pct,
  COUNT(*) AS num_companies
FROM layoffs
WHERE (percentage_laid_off IS NOT NULL AND stage IS NOT NULL)
GROUP BY stage
ORDER BY avg_laid_off_pct DESC;
-- Country Summary of Total Laid Off and Percentage 
SELECT 
  country,
  COUNT(*) AS num_events,
  SUM(total_laid_off) AS total_laid_off,
  AVG(percentage_laid_off) AS avg_pct_laid_off
FROM layoffs
WHERE total_laid_off IS NOT NULL AND percentage_laid_off IS NOT NULL
GROUP BY country
ORDER BY total_laid_off DESC;
-- Industry Specific Layoff Rates (Top 10)
SELECT 
  industry,
  SUM(total_laid_off) AS total_laid_off,
  AVG(percentage_laid_off) AS avg_pct_laid_off
FROM layoffs
GROUP BY industry
ORDER BY total_laid_off DESC
LIMIT 10;

