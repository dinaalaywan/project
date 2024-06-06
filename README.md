-- Deleting duplicates 

CREATE TABLE layoffs_staging
LIKE world_layoffs.layoffs ;

SELECT * 
FROM layoffs_staging;

INSERT layoffs_staging 
SELECT *
FROM layoffs


SELECT  *,
ROW_NUMBER () OVER (
PARTITION BY 
company, location , industry , total_laid_off , percentage_laid_off , `date` , stage, country, funds_raised_millions ) AS row_num
FROM layoffs_staging ;

WITH duplicate_cte AS 
(
SELECT  *,
ROW_NUMBER () OVER (
PARTITION BY 
company, location , industry , total_laid_off , percentage_laid_off , `date` , stage, country, funds_raised_millions ) AS row_num
FROM layoffs_staging 
)
SELECT * 
FROM duplicate_cte 
WHERE row_num >1 ;





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



SELECT * FROM layoffs_staging2 
WHERE row_num > 1;



INSERT INTO layoffs_staging2 
SELECT  *,
ROW_NUMBER () OVER (
PARTITION BY 
company, location , industry , total_laid_off , percentage_laid_off , `date` , stage, country, funds_raised_millions ) AS row_num
FROM layoffs_staging ;



DELETE
FROM layoffs_staging2
WHERE row_num > 1 ;

SET SQL_SAFE_UPDATES = 0;

SELECT * 
FROM layoffs_staging2 
WHERE row_num > 1;




-- standardizing data 

SELECT company, trim(company) 
FROM layoffs_staging2 ;

UPDATE layoffs_staging2
SET company = trim(company);

SELECT *
FROM layoffs_staging2 
WHERE industry LIKE 'crypto%'
;

UPDATE layoffs_staging2
SET industry = 'crypto'
WHERE industry LIKE 'crypto%';

SELECT distinct(industry)
FROM layoffs_staging2 ;

SELECT *
FROM layoffs_staging2 
WHERE country LIKE 'united states%'
ORDER BY 1;

SELECT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1 ;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'united states%' ;

SELECT *
FROM layoffs_staging2;

SELECT `date`
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET date = STR_TO_DATE(`date`, '%m/%d/%Y')

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE ;




-- NULLS and blank values 

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL ;

SELect *
FROM layoffs_staging2
WHERE company= 'Airbnb';

UPDATE layoffs_staging2
SET industry = NULL 
WHERE industry = '';

SELECT t1.industry,t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company= t2.company 
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL ;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company= t2.company
    SET t1.industry= t2.industry
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL ;

DELETE FROM layoffs_staging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;
