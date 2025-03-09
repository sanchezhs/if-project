import os
import pandas as pd
import re
from bs4 import BeautifulSoup

base_sale_url = (
    "https://www.idealista.com/press-room/property-price-reports/sale/1/report/%s/"
)
alternative_sale_url = (
    "https://www.idealista.com/sala-de-prensa/informes-precio-vivienda/venta/report/%s/"
)
base_rent_url = (
    "https://www.idealista.com/press-room/property-price-reports/rent/1/report/%s/"
)
alternative_rent_url = (
    "https://www.idealista.com/sala-de-prensa/informes-precio-vivienda/alquiler/report/%s/"
)
headers = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:135.0) Gecko/20100101 Firefox/135.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.8,en-US;q=0.5,en;q=0.3",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Referer": "https://www.idealista.com/sala-de-prensa/informes-precio-vivienda/venta/report/",
    "Connection": "keep-alive",
}
available_years = range(2006, 2024 + 1)
idealista_ccaa = {
    "Andalucía": "Andalucía",
    "Aragón": "Aragón",
    "Asturias": "Asturias, Principado de",
    "Baleares": "Balears, Illes",
    "Canarias": "Canarias",
    "Cantabria": "Canarias",
    "Castilla y León": "Castilla y León",
    "Castilla-La Mancha": "Castilla-La Mancha",
    "Cataluña": "Cataluña",
    "Ceuta": "Ceuta",
    "Comunitat Valenciana": "Comunitat Valenciana",
    "Euskadi": "País Vasco",
    "Extremadura": "Extremadura",
    "Galicia": "Galicia",
    "La Rioja": "Rioja, La",
    "Madrid Comunidad": "Madrid, Comunidad de",
    "Melilla": "Melilla",
    "Murcia Región": "Murcia, Región de",
    "Navarra": "Navarra, Comunidad Foral de",
}

def parse_htmls():
    def parse_directory(directory, is_rent):
        all_data = []
        for file in os.listdir(directory):
            html_content = open(f"{directory}/{file}", "r").read()
            try:
                ccaas_data = parse_table(html_content)

                for data in ccaas_data:
                    if data["comunidad_autonoma"] in idealista_ccaa:
                        data["comunidad_autonoma"] = idealista_ccaa[data["comunidad_autonoma"]]
                    else:
                        raise ValueError(f"Unknown comunidad autónoma: {data['comunidad_autonoma']}")
                    data["precio_m2"] = data.get("precio_m2", None)
                    data["año"] = data.get("año", None)
                    data["variacion_mensual"] = data.get("variacion_mensual", None)
                    data["variacion_trimestral"] = data.get("variacion_trimestral", None)
                    data["variacion_anual"] = data.get("variacion_anual", None)
                    data["es_alquiler"] = is_rent
                    all_data.append(data)
            except Exception as e:
                print(f"Error parsing {file}: {e}")
        return all_data

    sale_dir = "data/precios_vivienda/venta"
    rent_dir = "data/precios_vivienda/alquiler"
    print("Parsing sale HTMLs...")
    sale_data = parse_directory(sale_dir, is_rent=False)
    print("Parsing rent HTMLs...")
    rent_data = parse_directory(rent_dir, is_rent=True)

    all_data = sale_data + rent_data
    df = pd.DataFrame(all_data)
    df.to_csv("data/precios_vivienda/precios_vivienda.csv", index=False)
    print("Data saved to CSV")

def parse_table(html_content):
    soup = BeautifulSoup(html_content, "html.parser")
    table = soup.find("table")

    if table:
        all_data = []
        title = soup.find("title").text
        title_year = re.search(r"\d{4}", title).group(0)

        rows = table.find_all("tr", class_="indent-1")
        for row in rows:
            cols = row.find_all("td")
            comunidad = cols[0].text.strip()
            precio_m2 = (
                cols[1]
                .text.strip()
                .replace("€/m2", "")
                .replace(".", "")
                .replace(",", ".")
                .strip()
            )
            variacion_mensual = (
                cols[2]
                .text.strip()
                .replace("%", "")
                .replace(",", ".")
                .replace("+ ", "")
                .replace("- ", "-")
                .strip()
            )

            variacion_trimestral = (
                cols[3]
                .text.strip()
                .replace("%", "")
                .replace(",", ".")
                .replace("+ ", "")
                .replace("- ", "-")
                .strip()
            )

            variacion_anual = (
                cols[4]
                .text.strip()
                .replace("%", "")
                .replace(",", ".")
                .replace("+ ", "")
                .replace("- ", "-")
                .strip()
            )

            ccaa_data = {
                "comunidad_autonoma": comunidad,
                "año": int(title_year),
                "precio_m2": float(precio_m2),
                "variacion_mensual": float(variacion_mensual) if variacion_mensual != "n.d." else None,
                "variacion_trimestral": float(variacion_trimestral) if variacion_trimestral != "n.d." else None,
                "variacion_anual": float(variacion_anual) if variacion_anual != "n.d." else None,
            }
            all_data.append(ccaa_data)
        return all_data
    else:
        raise ValueError("No table found in HTML")


if __name__ == "__main__":
    parse_htmls()
