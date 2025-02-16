import pandas as pd

from src.etl.transform import get_final_df_on_mongodb_and_postgres_data


final_df = get_final_df_on_mongodb_and_postgres_data()




if __name__=="__main__":
    print(final_df.info())