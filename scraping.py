import marimo

__generated_with = "0.15.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import pathlib as pl
    import json

    import httpx
    from bs4 import BeautifulSoup
    import ibis as ib
    from ibis import _
    from loguru import logger 


    ib.options.interactive = True

    DATA_C = pl.Path("data/colllected")
    DATA_P = pl.Path("data/processed")
    return BeautifulSoup, DATA_P, httpx, ib, json, logger


@app.cell
def _(BeautifulSoup, DATA_P, httpx, ib, json):
    BASE_URL = "https://www.transportprojects.org.nz/cycle-data"


    def get_counters(url: str = BASE_URL) -> ib.Table:
        """
        Return a table of information about Wellington's cycle counters with the columns

        - 'name': public facing name of counter, e.g. 'Aro Street (city bound)'
        - 'counter_id': str, counter ID of counter, e.g. '100041856'
        - 'longitude': WGS84 longitude of counter
        - 'latitude': WGS84 latitude of counter
        """
        with httpx.Client() as client:
            # Get counter names and IDs
            html = client.get(url).text
            soup = BeautifulSoup(html, "html.parser")
            records = []
            for opt in soup.find(id="Form_CounterFilterForm_MarkerID").find_all("option"):
                name = opt.string.strip()
                cid = (opt.get("value") or "").strip()
                if not cid or cid.lower() == "all":
                    continue
                records.append(dict(counter_name=name, counter_id=cid))

            # Get geolocation of each counter
            url = f"{BASE_URL}/showdata"
            params = {
                "DataSource": "electronic",
                "MarkerID": None,
                "Month": f"2025-03-01",
            }
            new_records = []
            for record in records:
                params["MarkerID"] = record["counter_id"]
                try:
                    d = client.get(url, params=params).json()["MapData"]
                    record["longitude"] = d["long"]
                    record["latitude"] = d["lat"]
                except KeyError:
                    record["longitude"] = None
                    record["latitude"] = None
                new_records.append(record)

        return ib.memtable(new_records).distinct().order_by("counter_name")


    def counters_to_geojson(counters: ib.Table) -> dict:
        features = [
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [row.longitude, row.latitude]},
                "properties": {"name": row.counter_name, "counter_id": row.counter_id},
            }
            for row in counters.to_pandas().itertuples()
        ]
        return {"type": "FeatureCollection", "features": features}

    def process_counters(
        csv_path=DATA_P / "counters.csv",
        geojson_path=DATA_P / "counters.geojson",
        download_afresh: bool = False,
        as_geojson: bool = False,
    ) -> ib.Table:
        """
        Get all the Wellington cycle counter data, save them to
        the given CSV and GeoJSON paths, and return the resulting table
        or, if ``as_geojson``, the (decoded) GeoJSON FeatureCollection.
    
        If the data already exists and not ``download_afresh``, 
        then just load it from the paths in the selected format.
        """
        if not csv_path.exists() or download_afresh:
            counters = get_counters()
            counters.to_csv(csv_path)
            with geojson_path.open("w") as tgt:
                json.dump(counters_to_geojson(counters), tgt, indent=2)
        elif as_geojson:
            with geojson_path.open() as src:
                counters = json.load(src)
        else:
            counters = ib.read_csv(csv_path)
        return counters


    def get_dates(url: str = BASE_URL) -> list:
        """
        Return a list of YYYY-MM datestrings of data months available.
        """
        html = httpx.get(url).text
        soup = BeautifulSoup(html, "html.parser")
        results = []
        for opt in soup.find(id="Form_CounterFilterForm_Month").find_all("option"):
            date = (opt.get("value") or "").strip()
            if not date:
                continue
            results.append(date[:7])

        return sorted(set(results))[::-1]


    def get_counts(
        counter_id: str, year: int, month: int, base_url: str = BASE_URL, httpx_client=None
    ) -> ib.Table:
        # https://www.transportprojects.org.nz/cycle-data/showdata/?DataSource=electronic&MarkerID=100041855&Month=2025-05-01

        url = f"{BASE_URL}/showdata"
        params = {
            "DataSource": "electronic",
            "MarkerID": counter_id,
            "Month": f"{year}-{month:02d}-01",
        }
        if httpx_client is None:
            client = httpx.Client()
        else:
            client = httpx_client

        result = dict(
            counter_id=counter_id, 
            date=f"{year}-{month:02d}",  
            count_month=None,
            count_weekday_avg=None, 
            count_weekend_avg=None,
        )
        try:
            html = client.get(url, params=params).raise_for_status().json()["HTML"]
        except Exception:
            return result
                
        soup = BeautifulSoup(html, "html.parser")

        # Month count
        els = soup.select("div.cycle-data__circle-liner.cycle-data__circle-liner--small h3")
        if els:
            count = int(els[0].string.replace(",", ""))
        else:
            count = None
        result["count_month"] = count

        # Daily averages
        els = soup.select("div.cycle-data__column div.cycle-data__figure p.cycle-data__figure-number")
        if els:
            wda = int(els[-2].string.replace(",", ""))
            wea = int(els[-1].string.replace(",", ""))
        else:
            wda, wea = None, None
        result["count_weekday_avg"] = wda
        result["count_weekend_avg"] = wea

        return result
    
    return get_counts, get_dates, process_counters


@app.cell
def _(get_dates, process_counters):
    dates = get_dates()
    print(dates) 
    counters = process_counters()
    counters

    return counters, dates


@app.cell
def _(DATA_P, counters, dates, get_counts, httpx, ib, logger):
    # Download and save counts
    with httpx.Client() as client:
        for date in dates:
            logger.info(f"Working on {date}")
            records = []
            for row in counters.to_pandas().itertuples():
                year, month = [int(x) for x in date.split("-")]
                counts = get_counts(
                    counter_id=row.counter_id, year=year, month=month, httpx_client=client
                )
                records.append(counts)

            counts = counters.select("counter_name", "counter_id").join(
                ib.memtable(records), "counter_id"
            )
            counts.to_csv(DATA_P / f"counts_{date.replace('-', '')}.csv")
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
