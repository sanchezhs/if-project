import pandas as pd

airbnb_files = [
    "data/airbnb/barcelona_listings.csv",
    "data/airbnb/madrid_listings.csv",
    "data/airbnb/malaga_listings.csv",
    "data/airbnb/sevilla_listings.csv",
    "data/airbnb/valencia_listings.csv",
    "data/airbnb/girona_listings.csv",
    "data/airbnb/mallorca_listings.csv",
    "data/airbnb/menorca_listings.csv",
    "data/airbnb/euskadi_listings.csv",
]

airbnb_dataframes = []

for file in airbnb_files:
    try:
        df = pd.read_csv(file)
        df["provincia"] = (
            file.split("/")[-1].replace("_listings.csv", "").capitalize()
        )  # Extraer provincia del nombre de archivo
        airbnb_dataframes.append(df)
    except Exception as e:
        print(f"Error cargando {file}: {e}")

df_airbnb = pd.concat(airbnb_dataframes, ignore_index=True)

df_airbnb.to_csv("data/airbnb/delete.csv", index=False)
