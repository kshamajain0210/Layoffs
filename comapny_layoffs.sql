
-- Dataset from :-  https://www.kaggle.com/datasets/swaptr/layoffs-2022

-- Schema name - world_layoffs
-- Table name - layoffs

SELECT * FROM world_layoffs.layoffs;

-- Creating a copy of a table for changes 
CREATE TABLE world_layoffs.layoffs_staging LIKE world_layoffs.layoffs;

-- Insert data in new table
INSERT layoffs_staging SELECT * FROM world_layoffs.layoffs;


-- Will do below task for data cleaning
-- 1. check and remove rows if any duplicates
-- 2. standardize data and fix errors
-- 3. check null values 
-- 4. remove unnecessary columns and rows

SELECT * FROM world_layoffs.layoffs_staging;

-- 1. check and remove rows if any duplicates

-- To check the duplicates rows by using row number function 

SELECT *
FROM (
	SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
	ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions) AS row_num
	FROM world_layoffs.layoffs_staging
) duplicates
WHERE row_num > 1;



-- To delete we need to create a new column and add those row numbers in. Then delete where row numbers are over 2, then delete that column

ALTER TABLE world_layoffs.layoffs_staging ADD row_num INT;


SELECT * FROM world_layoffs.layoffs_staging;

CREATE TABLE `world_layoffs`.`layoffs_staging2` (
`company` text,
`location`text,
`industry`text,
`total_laid_off` INT,
`percentage_laid_off` text,
`date` text,
`stage`text,
`country` text,
`funds_raised_millions` int,
row_num INT
);

INSERT INTO `world_layoffs`.`layoffs_staging2`
(`company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
`row_num`)
SELECT *,
		ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions) AS row_num
FROM world_layoffs.layoffs_staging;

-- To remove duplicates rows, delete row_num is greater than 2

DELETE FROM layoffs_staging2 WHERE row_num >= 2;


-- 2. standardize data and fix errors

SELECT * FROM layoffs_staging2;
 
-- Fixing null and blank values on industry column, updating blank values as NULL
SELECT DISTINCT industry FROM layoffs_staging2 ORDER BY industry;
SELECT * FROM layoffs_staging2 WHERE industry IS NULL OR industry = '' ORDER BY industry;
UPDATE layoffs_staging2 SET industry = NULL WHERE industry = '';

-- now we need to populate those nulls value in industry column, for that we check if there is a industry value for other and copy that industry value where it is blank

UPDATE layoffs_staging2 t1 JOIN layoffs_staging2 t2 ON t1.company = t2.company SET t1.industry = t2.industry
WHERE t1.industry IS NULL AND t2.industry IS NOT NULL;

-- Crypto has multiple variations in industry column, updating to one
SELECT DISTINCT industry FROM layoffs_staging2 ORDER BY industry;
UPDATE layoffs_staging2 SET industry = 'Crypto' WHERE industry IN ('Crypto Currency', 'CryptoCurrency');

-- In country there is mistake by dot will eliminate that "United States." to "United States" 
SELECT DISTINCT country FROM layoffs_staging2 ORDER BY country;
UPDATE layoffs_staging2 SET country = TRIM(TRAILING '.' FROM country);

-- Using str to date to update this field
UPDATE layoffs_staging2 SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- Date is in string format will change it to date format
ALTER TABLE layoffs_staging2 MODIFY COLUMN `date` DATE;


-- 3. check null values 
-- the null values in all the column and specially for total_laid_off, percentage_laid_off, and funds_raised_millions all look normal.

--  4. remove unnecessary columns and rows

-- deleting the rows where we can't retrieve any useful information
DELETE FROM layoffs_staging2 WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

-- Deleting extra column we created to identify duplicates
ALTER TABLE layoffs_staging2 DROP COLUMN row_num;



-- EDA QUERIES- After DATA CLEANING 
-- To find the max value from total_laid_off column
SELECT MAX(total_laid_off) FROM layoffs_staging2;

--  Percentage to see how big these layoffs were
SELECT MAX(percentage_laid_off),  MIN(percentage_laid_off) FROM layoffs_staging2 WHERE  percentage_laid_off IS NOT NULL;

-- Which companies had 100 percent of laid off
SELECT * FROM layoffs_staging2 WHERE  percentage_laid_off = 1;

-- order by funds_raised_millions to see how big some of these companies were
SELECT * FROM layoffs_staging2 WHERE  percentage_laid_off = 1 ORDER BY funds_raised_millions DESC;

-- 5 Companies with the biggest single Layoff

SELECT company, total_laid_off FROM layoffs_staging2 ORDER BY 2 DESC LIMIT 5;

-- 10 Companies with the most Total Layoffs
SELECT company, SUM(total_laid_off) FROM layoffs_staging2 GROUP BY company ORDER BY 2 DESC LIMIT 10;

-- 10 location with the most Total Layoffs
SELECT location, SUM(total_laid_off) FROM layoffs_staging2 GROUP BY location ORDER BY 2 DESC LIMIT 10;

-- Total in the past 4 years
SELECT YEAR(date), SUM(total_laid_off) FROM layoffs_staging2 GROUP BY YEAR(date) ORDER BY 1 ASC;


