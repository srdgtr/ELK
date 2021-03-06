import configparser
import os
import sys
from datetime import datetime
from ftplib import FTP
from pathlib import Path

import dropbox
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL

sys.path.insert(0, str(Path.home()))
from bol_export_file import get_file

alg_config = configparser.ConfigParser()
alg_config.read(Path.home() / "general_settings.ini")
dbx_api_key = alg_config.get("dropbox", "api_dropbox")
dbx = dropbox.Dropbox(dbx_api_key)
config_db = dict(
    drivername="mariadb",
    username=alg_config.get("database leveranciers", "user"),
    password=alg_config.get("database leveranciers", "password"),
    host=alg_config.get("database leveranciers", "host"),
    port=alg_config.get("database leveranciers", "port"),
    database=alg_config.get("database leveranciers", "database"),
)
engine = create_engine(URL.create(**config_db))
current_folder = Path.cwd().name.upper()
export_config = configparser.ConfigParser(interpolation=None)
export_config.read(Path.home() / "bol_export_files.ini")
korting_percent = int(export_config.get("stap 1 vaste korting", current_folder.lower()).strip("%"))

date_now = datetime.now().strftime("%c").replace(":", "-")

def get_info_file():
    with FTP(host=alg_config.get("pieterman ftp", "server")) as ftp:
        ftp.login(user=alg_config.get("pieterman ftp", "user"), passwd=alg_config.get("pieterman ftp", "passwd"))
        # ftp.cwd("")
        # ftp.retrlines('LIST')
        file_name = "csvgi.csv"

        with open("ELK_N_" + date_now + ".csv", "wb") as f:
            ftp.retrbinary("RETR " + file_name, f.write)


get_info_file()

elk_voorraad_info = (
    pd.read_csv(
        max(Path.cwd().glob("ELK_N_*.csv"), key=os.path.getctime), sep="^", encoding="ISO-8859-1", dtype={"Artnr": object}
    )
    .rename(
        columns={
            "Artnr": "sku",
            "Merk": "brand",
            "Omschrijving": "info",
            "EanCode": "ean",
            "VerkoopPrijs": "price",
            "Categorie": "group",
            "Voorraad": "stock",
            "Eigenschap": "eigenschappen",
            "Gew.": "gewicht",
            "FTP": "Afbeelding",
        }
    )
    .drop_duplicates("sku")
    .assign(
        id=lambda x: x["ShortCde"].fillna(x["OrigNr"]),
        eigen_sku=lambda x: "ELK" + x["sku"],
        eigenschappen=(
            lambda x: x["eigenschappen"]
            .str.replace("\[vrij\] ", "", regex=True)
            .str.replace("\[vrij\]", "", regex=True)
            .str.replace("<br>", "", regex=True)
            .str.replace("&nbsp;", "", regex=True)
            .str.encode("ascii", "ignore")
            .str.decode("ascii")
        ),
        price=lambda x: x["price"].str.replace(",", ".").astype(float),
        price_advice=lambda x: x["AdviesPrijs"].str.replace(",", ".", regex=True),
        group=lambda x: x["group"].str.split("\\").str[:-1].str.join("::"),
        brand=lambda x: x["brand"].str.title(),
        url_artikel="",
        stock=lambda x: x["stock"].str.replace("[^0-9]", "", regex=True).astype(int),
        lk=lambda x: (korting_percent * x["price"] / 100).round(2),
    )
    .query("stock > 0")
    .query("ean > 10000000")
    .query("BestEenh < 20")
    .assign(
        stock=lambda x: np.where(x["stock"] > 25, 25, x["stock"]),  # om riciso te beperken max 25
        BestEenh=lambda x: np.where(x["BestEenh"] == 0, 1, x["BestEenh"]),  # vanwege min bestel hoeveelheid
        price=lambda x: (x["price"] - x["lk"]).round(2),
        price_advice=lambda x: round(x["price_advice"].astype(float), 2),
    )
)

elk_voorraad_info = elk_voorraad_info[~elk_voorraad_info["group"].str.startswith(("Auto", "Fiets", "::"))]

elk_voorraad_info_basis = elk_voorraad_info[
    [
        "sku",
        "ean",
        "id",
        "stock",
        "price",
        "price_advice",
        "info",
        "brand",
        "group",
        "eigenschappen",
        "BestEenh",
        "gewicht",
        "Afbeelding",
        "lk",
    ]
]
elk_voorraad_info_basis.to_csv("ELK_P_" + date_now + ".csv", index=False)

elk_info = elk_voorraad_info.rename(
    columns={
        "price": "prijs",
        "brand": "merk",
        "group": "category",
        "info": "product_title",
        "eigenschappen": "lange_omschrijving",
        "BestEenh": "verpakings_eenheid",
        "Afbeelding": "url_plaatje",
        "stock": "voorraad",
        "price_advice": "advies_prijs",
    }
)

latest_file = max(Path.cwd().glob("ELK_P_*.csv"), key=os.path.getctime)
with open(latest_file, "rb") as f:
    dbx.files_upload(
        f.read(), "/macro/datafiles/ELK/" + latest_file.name, mode=dropbox.files.WriteMode("overwrite", None), mute=True
    )

elk_info_db = elk_info[
    [
        "eigen_sku",
        "sku",
        "ean",
        "voorraad",
        "merk",
        "prijs",
        "advies_prijs",
        "category",
        "gewicht",
        "url_plaatje",
        "url_artikel",
        "product_title",
        "lange_omschrijving",
        "verpakings_eenheid",
        "lk",
    ]
]

current_folder = Path.cwd().name.upper()
huidige_datum = datetime.now().strftime("%d_%b_%Y")
elk_info_db.to_sql(f"{current_folder}_dag_{huidige_datum}", con=engine, if_exists="replace", index=False, chunksize=1000)

with engine.connect() as con:
    con.execute(f"ALTER TABLE {current_folder}_dag_{huidige_datum} ADD PRIMARY KEY (eigen_sku(20))")
    aantal_items = con.execute(f"SELECT count(*) FROM {current_folder}_dag_{huidige_datum}").fetchall()[-1][-1]
    totaal_stock = int(con.execute(f"SELECT sum(voorraad) FROM {current_folder}_dag_{huidige_datum}").fetchall()[-1][-1])
    totaal_prijs = int(con.execute(f"SELECT sum(prijs) FROM {current_folder}_dag_{huidige_datum}").fetchall()[-1][-1])
    leverancier = f"{current_folder}"
    sql_insert = (
        "INSERT INTO process_import_log (aantal_items, totaal_stock, totaal_prijs, leverancier) VALUES (%s,%s,%s,%s)"
    )
    con.execute(sql_insert, (aantal_items, totaal_stock, totaal_prijs, leverancier))

engine.dispose()
