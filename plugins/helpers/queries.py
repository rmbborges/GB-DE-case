class SqlQueries:
    GENERAL_MONTHLY_SALES_TABLE_QUERY = """
        SELECT
            EXTRACT(YEAR FROM DATA_VENDA) AS year,
            EXTRACT(MONTH FROM DATA_VENDA) AS month,
            SUM(QTD_VENDA) AS sold_units_amount
        FROM
            ${raw_sales_dataset}.${raw_sales_table}
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
            ${raw_sales_dataset}.${raw_sales_table}
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
            ${raw_sales_dataset}.${raw_sales_table}
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
            ${raw_sales_dataset}.${raw_sales_table}
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
            ${datawarehouse_sales_dataset}.product_monthly_sales
        WHERE 
            year = ${year}
            AND month = ${month}
        QUALIFY
            ROW_NUMBER() OVER(ORDER BY sold_units_amount DESC) = 1
    """

    DATAWAREHOUSE_TWEETS_QUERY = """
        SELECT
            id,
            author_id,
            created_at,
            text,
            searched_product,
            public_metrics
        FROM
            ${raw_twitter_dataset}.${raw_tweets_table}
        QUALIFY
            ROW_NUMBER() OVER(PARTITION BY id ORDER BY ingested_at DESC) = 1
    """
 