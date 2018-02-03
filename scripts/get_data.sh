#!env bash

wget -cO bicis.zip https://data.buenosaires.gob.ar/api/datasets/rk7pYtZQke/download
wget -cO bicis-trayectos.zip https://data.buenosaires.gob.ar/api/datasets/HJ8rdKWmJl/download

mkdir data
unzip -o bicis.zip -d data
unzip -o bicis-trayectos.zip -d data/