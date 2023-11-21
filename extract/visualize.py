import matplotlib.pyplot as plt
import pandas as pd

if __name__ == '__main__':
    df = pd.read_csv("data/history/flightsHistory_2022-10-18_to_2023-10-15.csv")
    df = df.groupby(['airline_iata']).size().sort_values(ascending=True).reset_index(name='counts')
    df.set_index('airline_iata', inplace=True)

# Calculate the percentage for each category
    df['percentage'] = df['counts'] / df['counts'].sum() * 100

    # Create a new DataFrame with only the categories above 1% and "others"
    threshold = 3
    df_filtered = df[df['percentage'] >= threshold].copy()
    df_filtered.loc['others'] = df[df['percentage'] < threshold].sum()

    df_filtered.info()
    print(df_filtered)
    ax = df_filtered.plot(kind='pie', y='counts', autopct='%1.1f%%', startangle=90, legend='', ylabel='')
    plt.title("Split of airlines in the dataset")
    plt.show()
