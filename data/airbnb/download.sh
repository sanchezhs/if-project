#!/bin/bash

set -xe

function download_airbnb() {
  local filename="${1,,}_listings.csv"
  echo "Downloading ${filename}"
  wget -O "$filename" "$2"
  echo "${filename} OK"
}

download_airbnb "Barcelona" "https://data.insideairbnb.com/spain/catalonia/barcelona/2024-12-12/visualisations/listings.csv"
download_airbnb "Euskadi" "https://data.insideairbnb.com/spain/pv/euskadi/2024-12-31/visualisations/listings.csv"
download_airbnb "Girona" "https://data.insideairbnb.com/spain/catalonia/girona/2024-12-31/visualisations/listings.csv"
download_airbnb "Madrid" "https://data.insideairbnb.com/spain/comunidad-de-madrid/madrid/2024-12-12/visualisations/listings.csv"
download_airbnb "Malaga" "https://data.insideairbnb.com/spain/andaluc%C3%ADa/malaga/2024-12-31/visualisations/listings.csv"
download_airbnb "Mallorca" "https://data.insideairbnb.com/spain/islas-baleares/mallorca/2024-12-14/visualisations/listings.csv"
download_airbnb "Menorca" "https://data.insideairbnb.com/spain/islas-baleares/menorca/2024-12-31/visualisations/listings.csv"
download_airbnb "Sevilla" "https://data.insideairbnb.com/spain/andaluc%C3%ADa/sevilla/2024-12-31/visualisations/listings.csv"
download_airbnb "Valencia" "https://data.insideairbnb.com/spain/vc/valencia/2024-12-21/visualisations/listings.csv"
