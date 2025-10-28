import scrapy
import os
import re

from ..utils import *


# Known competition defaults to ensure we only scrape the intended season.
_DEFAULT_SEASON_RANGES = {
    ("vbl", "160"): (2025, 2026),  # 1. Bundesliga Männer 2025/26
    ("vbl", "185"): (2025, 2026),  # 1. Bundesliga Frauen 2025/26
}

class CompetitionMatchesSpider(scrapy.Spider):
    name = 'competition_matches'

    def __init__(
        self,
        fed_acronym='',
        competition_id='',
        competition_pid='',
        season_start_year=None,
        season_end_year=None,
        **kwargs,
    ):
        self.start_urls = [f'https://{fed_acronym}-web.dataproject.com/CompetitionMatches.aspx?ID={competition_id}&PID={competition_pid}']
        self.competition_id = competition_id
        self.competition_pid = competition_pid
        self.fed_acronym = fed_acronym
        self.season_start_year = int(season_start_year) if season_start_year else None
        self.season_end_year = int(season_end_year) if season_end_year else None

        if self.season_start_year is None and self.season_end_year is None:
            default_range = _DEFAULT_SEASON_RANGES.get(
                (self.fed_acronym, str(self.competition_id))
            )
            if default_range:
                self.season_start_year, self.season_end_year = default_range

        if self.season_start_year and self.season_end_year:
            if self.season_end_year < self.season_start_year:
                raise ValueError('season_end_year must be greater than or equal to season_start_year')
        match_id = ''
        match_date = ''
        match_location = ''
        home_team = ''
        home_points = ''
        guest_team = ''
        guest_points = ''
        self.first_item_date = ''
        self.last_item_date = ''
        self.items_scraped = 0

        super().__init__(**kwargs)

    def start_requests(self):        
        cookies = {f'CompetitionLangCode{self.fed_acronym}': 'en-GB'}
        yield scrapy.Request(self.start_urls[0], cookies=cookies, callback=self.parse)

    def parse(self, response):
        competition_items = []

        matches = response.xpath("//div[@id='printableArea']/div/div/div/div/div[position() >= 1]/div[2]/div")

        for match in matches:
            match_id_string = match.xpath("./div/div/div[5]/p/@onclick").get()
            match_id = re.search(r'mID=(\d+)', match_id_string).group(1)

            match_date_text = match.xpath("./div/div/div/p[1]/span[1]/text()").get()
            match_date = parse_short_date(match_date_text)

            match_year = None
            if match_date:
                try:
                    match_year = int(match_date.split('-')[0])
                except (ValueError, AttributeError):
                    match_year = None

            if match_year is not None:
                if self.season_start_year and match_year < self.season_start_year:
                    continue
                if self.season_end_year and match_year > self.season_end_year:
                    continue
            elif self.season_start_year or self.season_end_year:
                # Skip matches with unparsable dates when a season filter is active.
                continue

            match_location = match.xpath("./div/div/div/p[2]/span[1]/text()").get()
            if match_location:
                match_location = match_location.lower()

            home_team = match.xpath("./div/div/div[5]/p/span/*/text() | ./div/div/div[5]/p/span/text()").get()
            if home_team:
                home_team = home_team.lower()

            home_points = match.xpath("./div/div/div[7]/p[1]/span[1]/b/text()").get()

            guest_team = match.xpath("./div/div/div[9]/p/span/*/text() | ./div/div/div[9]/p/span/text()").get()
            if guest_team:
                guest_team = guest_team.lower()

            guest_points = match.xpath("./div/div/div[7]/p[1]/span[3]/b/text()").get()

            competition = {
                'Match ID': match_id,
                'Match Date': match_date,
                'Stadium': match_location,
                'Home Team': home_team,
                'Home Points': home_points,
                'Guest Team': guest_team,
                'Guest Points': guest_points,
            }

            competition_items.append(competition)
            self.items_scraped += 1
            yield competition

        if not competition_items:
            self.first_item_date = ''
            self.last_item_date = ''
            return

        sorted_dates = sorted(item['Match Date'] for item in competition_items if item['Match Date'])
        first_date = sorted_dates[0]
        last_date = sorted_dates[-1]

        regex = r"\b\d{4}\b"
        match_start = re.search(regex, first_date)
        match_final = re.search(regex, last_date)

        if match_start:
            self.first_item_date = match_start.group()

        if match_final:
             self.last_item_date = match_final.group()

    def closed(spider, reason):
        src = f'data/{spider.fed_acronym}-{spider.competition_id}-{spider.competition_pid}-competition_matches.csv'

        pid_segment = f'{spider.competition_pid}-' if spider.competition_pid else ''

        if not spider.items_scraped or not spider.first_item_date or not spider.last_item_date:
            if os.path.exists(src):
                print(
                    'volleystats: no matches met the season filter; '
                    f'leaving {src} unchanged'
                )
            else:
                print('volleystats: no competition matches were scraped')
            return

        dst = (
            f'data/{spider.fed_acronym}-{spider.competition_id}-{pid_segment}'
            f'{spider.first_item_date}-{spider.last_item_date}-competition-matches.csv'
        )

        try:
            os.rename(src, dst)

            print(f'volleystats: {dst} file was created')
        except(FileExistsError):
            print(f'volleystats: file {dst} already exists.\n{src} was created or renamed')