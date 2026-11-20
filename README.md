# news-briefing

`collect.py` fetches ~100 RSS feeds, Google Trends (AT/GB/US) and the weather
(Wien, Neuberg im Burgenland, Villach) and writes `data/briefing.json`.
GitHub Actions runs it every morning (04:00 Vienna) and evening (17:00 Vienna).
A Claude routine reads the JSON, curates it and sends the briefing e-mail.

Manual run: Actions tab -> collect-briefing-data -> Run workflow.
Raw data: https://raw.githubusercontent.com/mmarlovics-ats/news-briefing/main/data/briefing.json
