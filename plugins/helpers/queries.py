class SqlQueries:
    GENERAL_MONTHLY_SALES_TABLE_QUERY = """
        SELECT
            EXTRACT(YEAR FROM DATA_VENDA) AS year,
            EXTRACT(MONTH FROM DATA_VENDA) AS month,
            SUM(QTD_VENDA) AS sold_units_amount
        FROM
            raw__sales.sales
        GROUP BY 
            year,
            month
    """

    PRODUCT_GENERAL_SALES_TABLE_QUERY = """
        SELECT
            ID_MARCA AS brand_id,
            MARCA AS brand_name,
            ID_LINHA AS product_id,
            LINHA AS product_name,
            SUM(QTD_VENDA) AS sold_units_amount
        FROM
            raw__sales.sales
        GROUP BY 
            brand_id,
            brand_name,
            product_id,
            product_name
    """

    BRAND_MONTHLY_SALES_TABLE_QUERY = """
        SELECT
            EXTRACT(YEAR FROM DATA_VENDA) AS year,
            EXTRACT(MONTH FROM DATA_VENDA) AS month,
            ID_MARCA AS brand_id,
            MARCA AS brand_name,
            SUM(QTD_VENDA) AS sold_units_amount
        FROM
            raw__sales.sales
        GROUP BY 
            year,
            month,
            brand_id,
            brand_name
    """

    PRODUCT_MONTHLY_SALES_TABLE_QUERY = """
        SELECT
            EXTRACT(YEAR FROM DATA_VENDA) AS year,
            EXTRACT(MONTH FROM DATA_VENDA) AS month,
            ID_LINHA AS product_id,
            LINHA AS product_name,
            SUM(QTD_VENDA) AS sold_units_amount
        FROM
            raw__sales.sales
        GROUP BY 
            year,
            month,
            product_id,
            product_name
    """
 
    TOP_PRODUCT_QUERY = """
        SELECT
            product_name
        FROM
            silver__sales.product_monthly_sales
        WHERE 
            year = ${year}
            AND month = ${month}
        QUALIFY
            ROW_NUMBER() OVER(ORDER BY sold_units_amount DESC) = 1
    """
 