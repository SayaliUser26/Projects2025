import sys, sqlite3, pandas as pd

def main(db_path: str, out_csv: str) -> None:
    conn = sqlite3.connect(db_path)

    customers = pd.read_sql("SELECT customer_id AS Customer, age AS Age FROM customers WHERE age BETWEEN 18 AND 35;", conn)
    sales = pd.read_sql("SELECT sales_id, customer_id AS Customer FROM sales;", conn)
    orders = pd.read_sql("SELECT sales_id, item_id, quantity FROM orders;", conn)
    items = pd.read_sql("SELECT item_id, item_name AS Item FROM items;", conn)

    df = (orders.merge(items, on="item_id", how="left")
                 .merge(sales, on="sales_id", how="left")
                 .merge(customers, on="Customer", how="inner"))

    df["quantity"] = df["quantity"].fillna(0).astype(int)

    out = (df.groupby(["Customer", "Age", "Item"], as_index=False)["quantity"]
             .sum()
             .rename(columns={"quantity":"Quantity"}))

    out = out[out["Quantity"] > 0].sort_values(["Customer","Age","Item"]).reset_index(drop=True)
    out.to_csv(out_csv, index=False, sep=';')

if __name__ == "__main__":
    db_path = sys.argv[1] if len(sys.argv) > 1 else "Data Engineer - Assignment Database.db"
    out_csv = sys.argv[2] if len(sys.argv) > 2 else "output_pandas.csv"
    main(db_path, out_csv)
