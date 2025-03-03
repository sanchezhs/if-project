# Reprocess the data with correct alignment
import re
import pandas as pd


def txt_csv():
    data = []
    current_location = None

    header = ["Localización", "Precio m2 2024", "Variación mensual", "Variación trimestral", "Variación anual", "Máximo histórico", "Variación máximo"]
    numeric_columns = ["Precio m2 2024", "Variación mensual", "Variación trimestral", "Variación anual", "Máximo histórico", "Variación máximo"]

    file_path = "./precios_ventas_2024.txt"
    with open(file_path, "r", encoding="utf-8") as file:
        lines = file.readlines()

    for line in lines:
        line = line.strip()
        if not line:
            continue
        
        values = line.split("\t")

        # If the line has only one value, it's a location
        if len(values) == 1:
            current_location = values[0]
        elif current_location:
            row = [current_location] + values
            data.append(row)
            current_location = None  # Reset location after pairing with values

    # Create DataFrame again
    df = pd.DataFrame(data, columns=header)

    # Convert numerical values (removing currency symbols and formatting numbers correctly)
    def clean_numeric(value):
        return re.sub(r"[€/%]", "", value).replace(".", "").replace(",", ".").strip()

    for col in numeric_columns:
        df[col] = df[col].apply(clean_numeric)


    # Save as CSV
    csv_path = "data/precio_venta_2024_fixed.csv"
    df.to_csv(csv_path, sep=";", index=False, encoding="utf-8")

def csv_to_postgres():
    df = pd.read_csv("data/precio_venta_2024.csv", sep=",", encoding="utf-8")
    # column_mapping = {
    #     "Localización": "provincia",
    #     "Precio m2 2024": "precio_m2",
    #     "Variación mensual": "variacion_mensual",
    #     "Variación trimestral": "variacion_trimestral",
    #     "Variación anual": "variacion_anual",
    #     "Máximo histórico": "maximo_historico",
    #     "Variación máximo": "variacion_maximo"
    # }
    # df.rename(columns=column_mapping, inplace=True)

    # Borrar filas que contengan la palabra Provincia
    df = df[~df["provincia"].str.contains("provincia")]

    # Guardar
    df.to_csv("data/precio_venta_2024_fixed.csv", sep=";", index=False, encoding="utf-8")


if __name__ == "__main__":
    # txt_csv()
    csv_to_postgres()