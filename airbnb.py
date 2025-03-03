import pandas as pd

airbnb_files = [
    "data/barcelona_listings.csv",
    "data/madrid_listings.csv",
    "data/malaga_listings.csv",
    "data/sevilla_listings.csv",
    "data/valencia_listings.csv",
    "data/girona_listings.csv",
    "data/mallorca_listings.csv",
    "data/menorca_listings.csv",
    "data/euskadi_listings.csv"
]

airbnb_dataframes = []

for file in airbnb_files:
    try:
        df = pd.read_csv(file)
        df["provincia"] = file.split("/")[-1].replace("_listings.csv", "").capitalize()  # Extraer provincia del nombre de archivo
        airbnb_dataframes.append(df)
    except Exception as e:
        print(f"Error cargando {file}: {e}")

df_airbnb = pd.concat(airbnb_dataframes, ignore_index=True)

df_airbnb.to_csv("data/airbnb.csv", index=False)
