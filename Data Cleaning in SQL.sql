-- Data Cleaning


SELECT * 
FROM layoffs;

-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Null Values or Blank Values
-- 4. Remove Any Columns 

CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY COMPANY, INDUSTRY, TOTAL_LAID_OFF, PERCENTAGE_LAID_OFF, 'DATE') AS row_num
FROM layoffs_staging;


WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY COMPANY, INDUSTRY, TOTAL_LAID_OFF, PERCENTAGE_LAID_OFF, 'DATE', stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;


SELECT *
FROM layoffs_staging
WHERE company = 'Casper';





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

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY COMPANY, INDUSTRY, TOTAL_LAID_OFF, PERCENTAGE_LAID_OFF, 'DATE', stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

SELECT *
FROM layoffs_staging2 ;

DELETE
FROM layoffs_staging2 
WHERE row_num > 1;

-- standardizing data

UPDATE layoffs_staging2
SET company = trim(company);

SELECT company, industry
FROM layoffs_staging2 
where industry like 'Crypto%';

UPDATE layoffs_staging2
SET INDUSTRY = 'Crypto'
where industry like 'Crypto%';


SELECT distinct country, trim(trailing '.' from country)
FROM layoffs_staging2 
where country like 'United States%';

UPDATE layoffs_staging2
SET country = 'United States'
where country like 'United States%';

select `date`
from layoffs_staging2;

UPDATE layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

alter table layoffs_staging2
modify column `date` date;


select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

delete
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

select *
from layoffs_staging2
where industry is null
or industry = '';

Select * 
from layoffs_staging2;

Select *
from layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
    and t1.location = t2.location
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;

update layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null;


update layoffs_staging2
set industry = null
where industry = '';

alter table layoffs_staging2
drop column row_num;

select *
from layoffs_staging2
where percentage_laid_off = 1
order by funds_raised_millions desc;

select max(total_laid_off), max(percentage_laid_off)
from layoffs_staging2;

select year(`date`), sum(total_laid_off)
from layoffs_staging2
group by year(`date`)
order by 1 desc;

select substr(`date`,1,7) as `MONTH`, sum(total_laid_off)
from layoffs_staging2
where substr(`date`,1,7) is not null
group by `MONTH`
order by 1 asc
;

with rolling_total as 
(
select substr(`date`,1,7) as `MONTH`, sum(total_laid_off) as total_off
from layoffs_staging2
where substr(`date`,1,7) is not null
group by `MONTH`
order by 1 asc
)
select `MONTH`, total_off,
sum(total_off) over(order by `MONTH`) as Rolling_Total
from rolling_total;


select company, year(`date`), sum(total_laid_off)
from layoffs_staging2
group by company, year(`date`)
order by 3 desc;





with company_year (company, years, total_laid_off) as
(
select company, year(`date`), sum(total_laid_off)
from layoffs_staging2
group by company, year(`date`)
order by 3 desc
),company_year_rank as 
(
select *, dense_rank() over(partition by years order by total_laid_off desc) as ranking
from company_year
where years is not null
)
select * from company_year_rank
where ranking <= 5; 
