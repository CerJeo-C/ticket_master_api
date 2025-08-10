import requests
from datetime import datetime
import csv

def get_params(api_key, startDateTime, endDateTime, city, countryCode, size=200, sort="date,asc") -> dict:
    """
    Build the query parameter dictionary for the Ticketmaster Discovery API request.

    Parameters:
        api_key (str): Your Ticketmaster API key.
        startDateTime (str): Start date filter in ISO 8601 format with UTC 'Z' suffix (e.g., "2025-08-10T00:00:00Z").
        endDateTime (str): End date filter in ISO 8601 format with UTC 'Z' suffix.
        city (str): Name of the city to filter events by.
        countryCode (str): ISO 3166-1 alpha-2 country code (e.g., "CA" for Canada).
        size (int, optional): Number of results per page. Defaults to 200 (API maximum).
        sort (str, optional): Sort order for results (e.g., "date,asc"). Defaults to "date,asc".

    Returns:
        dict: Dictionary of query parameters ready to be passed to `requests.get()`.
    """
    query_params = {
        "apikey": api_key,
        "city": city,
        "countryCode": countryCode,
        "startDateTime": startDateTime,
        "endDateTime": endDateTime,
        "size": str(size),
        "sort": sort
    }
    return query_params


def get_iso_date(dt: datetime) -> str:
    """
    Convert a Python datetime object into an ISO 8601 date-time string
    with UTC 'Z' suffix, which is accepted by the Ticketmaster API.

    Args:
        dt (datetime): A Python datetime object representing the desired date and time.

    Returns:
        str: Date-time string in the format "YYYY-MM-DDTHH:MM:SSZ".
    """
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def extract_event_datetime(ev: dict) -> str:
    """
    Extract the event's start date-time from the Ticketmaster event object.

    This function first attempts to use the full ISO 'dateTime' value
    from the event's 'dates.start' field. If not available, it falls back
    to combining 'localDate' and 'localTime'. If no date is available, 
    it returns an empty string.

    Args:
        ev (dict): A single event dictionary from the Ticketmaster API response.

    Returns:
        str: An ISO-like date-time string, or an empty string if no date is available.
    """
    start = ev.get("dates", {}).get("start", {})
    if start.get("dateTime"):
        return start["dateTime"]
    local_date = start.get("localDate")
    local_time = start.get("localTime") or "00:00:00"
    return f"{local_date}T{local_time}" if local_date else ""


def fetch_all_events_to_csv(api_key: str, city: str, country: str,
                            start: datetime, end: datetime,
                            out_csv: str = "events.csv") -> None:
    """
    Fetch all events from the Ticketmaster Discovery API for the given city, country,
    and date range, and write them to a CSV file.

    Handles API pagination by following the 'next' link in responses until no more
    pages remain. Each row in the CSV will contain the event name and start date-time.

    Args:
        api_key (str): Your Ticketmaster API key.
        city (str): Name of the city to filter events by.
        country (str): ISO 3166-1 alpha-2 country code (e.g., "CA" for Canada).
        start (datetime): Start of the date range to filter events.
        end (datetime): End of the date range to filter events.
        out_csv (str, optional): Path/filename for the output CSV file. Defaults to "events.csv".

    Raises:
        RuntimeError: If the API returns a non-200 status code.
    """
    ROOT = "https://app.ticketmaster.com/discovery/v2/events.json"
    
    params = get_params(
        api_key=api_key,
        startDateTime=get_iso_date(start),
        endDateTime=get_iso_date(end),
        city=city,
        countryCode=country,
        size=200,
        sort="date,asc"
    )

    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["event_name", "event_datetime"])

        next_url = ROOT
        while next_url:
            resp = requests.get(next_url, params=params if next_url == ROOT else None)
            if resp.status_code != 200:
                raise RuntimeError(f"API error {resp.status_code}: {resp.text}")

            data = resp.json()
            # --- DEBUG BLOCK: REMOVE AFTER CHECKING ---
            if next_url == ROOT:
                print("Request URL:", resp.url)
                print("Top-level keys:", list(data.keys()))
                print("Page info:", data.get("page", {}))
                print("Has _embedded?", "_embedded" in data)
                if "errors" in data or "fault" in data:
                    print("API reported errors:", data.get("errors") or data.get("fault"))
            # --- END DEBUG BLOCK ---

            events = data.get("_embedded", {}).get("events", [])
            for ev in events:
                name = ev.get("name", "")
                when = extract_event_datetime(ev)
                writer.writerow([name, when])

            links = data.get("_links", {})
            next_link = links.get("next", {})
            next_url = next_link.get("href")
            params = None  # Only include initial params for the first request


def main():
    """
    Main entry point for the script.

    Sets up API key, search parameters, and calls the event fetch function
    to retrieve events for the specified city, country, and date range,
    saving them to 'events.csv'.
    """
    API_KEY = "" 
    city = "Calgary"
    country = "CA"
    start = datetime(2025, 10, 1, 0, 0)   
    end   = datetime(2025, 12, 1, 0, 0)   

    fetch_all_events_to_csv(API_KEY, city, country, start, end, out_csv="events.csv")
    print("Wrote events.csv")


if __name__ == "__main__":
    main()

