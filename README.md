# Filtering OSM

## Install dependencies

Debian:
```bash
sudo apt install -y osmium-tool
```
MacOs:
```zsh
brew install osmium-tool
```

## Download OSM File

```
wget https://download.geofabrik.de/north-america-latest.osm.pbf
```

## Run

```
osmium extract -v -c US-config.json data/north-america-latest.osm.pbf --overwrite   
```