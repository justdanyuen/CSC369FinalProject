import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

df = pd.read_csv('combined_features.csv')

df['co2_to_pop_density'] = df['co2_ppm'] / df['population_density']
df['pm25_to_pop_density'] = df['pm25_ugm3'] / df['population_density']
df['pm10_to_pop_density'] = df['pm10_ugm3'] / df['population_density']


ratios = ['co2_to_pop_density', 'pm25_to_pop_density', 'pm10_to_pop_density']
titles = ['CO2 to Population Density', 'PM2.5 to Population Density', 'PM10 to Population Density']

plt.figure(figsize=(15, 5))
for i, ratio in enumerate(ratios):
    plt.subplot(1, 3, i + 1)
    sns.histplot(df[ratio], kde=True, bins=30, color='skyblue')
    plt.title(titles[i])
    plt.xlabel(ratio)
    plt.ylabel('Frequency')

plt.tight_layout()

plt.show()
