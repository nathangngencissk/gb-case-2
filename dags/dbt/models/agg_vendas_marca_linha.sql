SELECT    SUM(QTD_VENDA) AS TOTAL_SALES,
          ID_MARCA,
          MARCA,
          ID_LINHA,
          LINHA
FROM      `base_vendas_dataset.base_vendas`
GROUP BY  2,3,4,5
ORDER BY  TOTAL_SALES DESC