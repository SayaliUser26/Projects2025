import sys, sqlite3, pandas as pd

def main(db_path: str, out_csv: str) -> None:
    conn = sqlite3.connect(db_path)
    query = """
    WITH base AS (
        SELECT
            s.customer_id AS Customer,
            c.age AS Age,
            i.item_name AS Item,
            COALESCE(o.quantity, 0) AS qty
        FROM sales s
        JOIN customers c ON c.customer_id = s.customer_id
        JOIN orders o ON o.sales_id = s.sales_id
        JOIN items i  ON i.item_id = o.item_id
        WHERE c.age BETWEEN 18 AND 35
    )
    SELECT
        Customer,
        Age,
        Item,
        CAST(SUM(qty) AS INTEGER) AS Quantity
    FROM base
    GROUP BY Customer, Age, Item
    HAVING SUM(qty) > 0
    ORDER BY Customer, Age, Item;
    """
    df = pd.read_sql(query, conn)
    df.to_csv(out_csv, index=False, sep=';')

if __name__ == "__main__":
    db_path = sys.argv[1] if len(sys.argv) > 1 else "Data Engineer - Assignment Database.db"
    out_csv = sys.argv[2] if len(sys.argv) > 2 else "output_sql.csv"
    main(db_path, out_csv)
