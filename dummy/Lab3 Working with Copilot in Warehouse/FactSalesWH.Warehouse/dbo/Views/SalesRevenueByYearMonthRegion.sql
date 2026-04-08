-- Auto Generated (Do not modify) 124EC0D5A8E4E2AFCF1EBEB2947B9D821A78AA5435A468A01724AB4AC618DD39
 
-- Step 1: Identify the required fields for grouping and sales revenue aggregation.
--   - Year, Month, MonthName: from [dbo].[DimDate]
--   - Sales Region: Use [CountryRegion] from [dbo].[DimCustomer] as the sales region
--   - Sales Revenue: SUM([SalesTotal]) from [dbo].[FactSalesOrder]

-- Step 2: Join [dbo].[FactSalesOrder] to [dbo].[DimDate] on [SalesOrderDateKey] = [DateKey]
-- Step 3: Join [dbo].[FactSalesOrder] to [dbo].[DimCustomer] on [CustomerKey]
-- Step 4: Group by Year, Month, MonthName, CountryRegion
-- Step 5: Create the view in the [dbo] schema as requested

CREATE VIEW [dbo].[SalesRevenueByYearMonthRegion]
AS
SELECT
    d.[Year],
    d.[Month],
    d.[MonthName],
    c.[CountryRegion] AS [SalesRegion],
    SUM(f.[SalesTotal]) AS [SalesRevenue]
FROM
    [dbo].[FactSalesOrder] f
    INNER JOIN [dbo].[DimDate] d
        ON f.[SalesOrderDateKey] = d.[DateKey]
    INNER JOIN [dbo].[DimCustomer] c
        ON f.[CustomerKey] = c.[CustomerKey]
GROUP BY
    d.[Year],
    d.[Month],
    d.[MonthName],
    c.[CountryRegion];