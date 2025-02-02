from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import os
import json
import time

from azure.storage.filedatalake import DataLakeServiceClient

remote_webdriver = 'remote_chromedriver'

options = Options()
options.add_argument('--headless')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')

account_name = "SA_NAME"
container_name = "CONTAINER_NAME"
adfs_sa_access_key = "ADFS_KEY"

service_client = DataLakeServiceClient(
    account_url=f"https://{account_name}.dfs.core.windows.net",
    credential=adfs_sa_access_key
)
filesystem_client = service_client.get_file_system_client(container_name)

def extract_tournament_data(url, tournament_id, season_id):

    with webdriver.Remote(
        command_executor=f'{remote_webdriver}:4444/wd/hub',
        options=options
    ) as driver:
        driver.get(url+f"{tournament_id}")

        driver.implicitly_wait(10)
        pre_element = driver.find_element(By.TAG_NAME, 'pre')
        api_response = pre_element.text

        api_response = json.loads(api_response)
  
        file_client = filesystem_client.get_file_client(f"/rawdata/tournament/tournament_{tournament_id}_{season_id}.json")

        file_client.upload_data(
            json.dumps(api_response, indent=4, ensure_ascii=False),
            overwrite=True
        )

        print("extract_tournament_data task executed.")
    
def extract_events_data(tournament_id, url):

    with webdriver.Remote(
        command_executor=f'{remote_webdriver}:4444/wd/hub',
        options=options
    ) as driver:
        for round_number in range(1,31):

            driver.get(url+f"{round_number}")
            driver.implicitly_wait(10)
            pre_element = driver.find_element(By.TAG_NAME, 'pre')
            api_response = pre_element.text

            api_response = json.loads(api_response)


            file_client = filesystem_client.get_file_client(f"/rawdata/events/events_{tournament_id}_{round_number}.json")

            file_client.upload_data(
                json.dumps(api_response, indent=4, ensure_ascii=False),
                overwrite=True
            )

            if round_number < 30:
                time.sleep(3)

        print("extract_events_data task executed.")
    
def extract_events_ids(tournament_id):

    events_ids = []

    filesystem_client = service_client.get_file_system_client(container_name)
    paths = filesystem_client.get_paths(path="/rawdata/events")

    file_paths = [path.name for path in paths]
    
    for file_path in file_paths:
        file_client = filesystem_client.get_file_client(file_path)
        download = file_client.download_file()
        content = download.readall().decode('utf-8')
        events_data = json.loads(content)
        for event_index in range(len(events_data['events'])):
                        if (events_data['events'][event_index]["status"]["description"]) == "Ended":
                            events_ids.append(events_data['events'][event_index]['id'])

    file_client = filesystem_client.get_file_client(f"/rawdata/events/events_{tournament_id}_ids.txt")

    content = "\n".join(str(event_id) for event_id in events_ids) + "\n"

    file_client.upload_data(content, overwrite=True)

    
    print("extract_events_ids task executed.")


def extract_events_lineups(url, tournament_id):

    with webdriver.Remote(
        command_executor=f'{remote_webdriver}:4444/wd/hub',
        options=options
    ) as driver:
        filesystem_client = service_client.get_file_system_client(container_name)
        file_client = filesystem_client.get_file_client(f"/rawdata/events/events_{tournament_id}_ids.txt")
        download = file_client.download_file()
        content = download.readall().decode('utf-8')
        events_ids = content.split("\n")

        for event_id in events_ids:
            driver.get(url+f"{event_id}"+"/lineups")
            driver.implicitly_wait(10)
            pre_element = driver.find_element(By.TAG_NAME, 'pre')
            api_response = pre_element.text

            api_response = json.loads(api_response)

            file_client = filesystem_client.get_file_client(f"/rawdata/events_lineups/events_{tournament_id}_{event_id}_lineups.json")

            file_client.upload_data(
                json.dumps(api_response, indent=4, ensure_ascii=False),
                overwrite=True
            )

            if event_id != events_ids[-1]:
                time.sleep(3)

    print("extract_events_lineups task executed.")


def extract_events_statistics(url, tournament_id):

    with webdriver.Remote(
        command_executor=f'{remote_webdriver}:4444/wd/hub',
        options=options
    ) as driver:
        filesystem_client = service_client.get_file_system_client(container_name)
        file_client = filesystem_client.get_file_client(f"/rawdata/events/events_{tournament_id}_ids.txt")
        download = file_client.download_file()
        content = download.readall().decode('utf-8')
        events_ids = content.split("\n")

        for event_id in events_ids:
            driver.get(url+f"{event_id}"+"/statistics")
            driver.implicitly_wait(10)
            pre_element = driver.find_element(By.TAG_NAME, 'pre')
            api_response = pre_element.text

            api_response = json.loads(api_response)

            file_client = filesystem_client.get_file_client(f"/rawdata/events_statistics/events_{tournament_id}_{event_id}_statistics.json")

            file_client.upload_data(
                json.dumps(api_response, indent=4, ensure_ascii=False),
                overwrite=True
            )

            if event_id != events_ids[-1]:
                time.sleep(3)

    print("extract_events_statistics task executed.")

def extract_events_best_players(url, tournament_id):

    with webdriver.Remote(
        command_executor=f'{remote_webdriver}:4444/wd/hub',
        options=options
    ) as driver:
        filesystem_client = service_client.get_file_system_client(container_name)
        file_client = filesystem_client.get_file_client(f"/rawdata/events/events_{tournament_id}_ids.txt")
        download = file_client.download_file()
        content = download.readall().decode('utf-8')
        events_ids = content.split("\n")

        for event_id in events_ids:
            driver.get(url+f"{event_id}"+"/best-players/summary")
            driver.implicitly_wait(10)
            pre_element = driver.find_element(By.TAG_NAME, 'pre')
            api_response = pre_element.text

            api_response = json.loads(api_response)

            file_client = filesystem_client.get_file_client(f"/rawdata/events_best_players/events_{tournament_id}_{event_id}_best_players.json")

            file_client.upload_data(
                json.dumps(api_response, indent=4, ensure_ascii=False),
                overwrite=True
            )

            if event_id != events_ids[-1]:
                time.sleep(3)

    print("extract_events_best_players task executed.")

def extract_events_incidents(url, tournament_id):

    with webdriver.Remote(
        command_executor=f'{remote_webdriver}:4444/wd/hub',
        options=options
    ) as driver:
        filesystem_client = service_client.get_file_system_client(container_name)
        file_client = filesystem_client.get_file_client(f"/rawdata/events/events_{tournament_id}_ids.txt")
        download = file_client.download_file()
        content = download.readall().decode('utf-8')
        events_ids = content.split("\n")

        for event_id in events_ids:
            driver.get(url+f"{event_id}"+"/incidents")
            driver.implicitly_wait(10)
            pre_element = driver.find_element(By.TAG_NAME, 'pre')
            api_response = pre_element.text

            api_response = json.loads(api_response)

            file_client = filesystem_client.get_file_client(f"/rawdata/events_incidents/events_{tournament_id}_{event_id}_incidents.json")

            file_client.upload_data(
                json.dumps(api_response, indent=4, ensure_ascii=False),
                overwrite=True
            )

            if event_id != events_ids[-1]:
                time.sleep(3)

    print("extract_events_incidents task executed.")

def extract_players(url, tournament_id, season_id):

    with webdriver.Remote(
        command_executor=f'{remote_webdriver}:4444/wd/hub',
        options=options
    ) as driver:
        offsets = [0, 100, 200, 300, 400]
        for offset in offsets:
            driver.get(url+f"{tournament_id}"+"/season/"+f"{season_id}"+f"/statistics?limit=100&order=-rating&offset={offset}&accumulation=total&group=summary")
            driver.implicitly_wait(10)
            pre_element = driver.find_element(By.TAG_NAME, 'pre')
            api_response = pre_element.text

            api_response = json.loads(api_response)

            file_client = filesystem_client.get_file_client(f"/rawdata/players/{tournament_id}_{season_id}_{offset}.json")

            file_client.upload_data(
                json.dumps(api_response, indent=4, ensure_ascii=False),
                overwrite=True
            )

            if offset != offsets[-1]:
                time.sleep(3)

    print("extract_players task executed.")


def extract_players_ids(tournament_id, season_id):

    players_ids = []

    filesystem_client = service_client.get_file_system_client(container_name)
    paths = filesystem_client.get_paths(path="/rawdata/players")

    file_paths = [path.name for path in paths]

    for file_path in file_paths:
        file_client = filesystem_client.get_file_client(file_path)
        download = file_client.download_file()
        content = download.readall().decode('utf-8')
        players_data = json.loads(content)
        for result_index in range(len(players_data['results'])):
            players_ids.append(players_data['results'][result_index]['player']['id'])

    file_client = filesystem_client.get_file_client(f"/rawdata/players/{tournament_id}_{season_id}_players_ids.txt")

    content = "\n".join(str(player_ids) for player_ids in players_ids) + "\n"

    file_client.upload_data(content,overwrite=True)

    print("extract_players_ids task executed.")


def extract_players_details(url, tournament_id, season_id):

    filesystem_client = service_client.get_file_system_client(container_name)
    file_client = filesystem_client.get_file_client(f"/rawdata/players/{tournament_id}_{season_id}_players_ids.txt")
    download = file_client.download_file()
    content = download.readall().decode('utf-8')
    players_ids = content.split("\n")

    with webdriver.Remote(
        command_executor=f'{remote_webdriver}:4444/wd/hub',
        options=options
    ) as driver:
        for player_id in players_ids:
                driver.get(url+f"{player_id}")
                driver.implicitly_wait(10)
                pre_element = driver.find_element(By.TAG_NAME, 'pre')
                api_response = pre_element.text

                api_response = json.loads(api_response)

                file_client = filesystem_client.get_file_client(f"/rawdata/players_details/{tournament_id}_{season_id}_{player_id}_details.json")

                file_client.upload_data(
                    json.dumps(api_response, indent=4, ensure_ascii=False),
                    overwrite=True
                )

                if player_id != players_ids[-1]:
                    time.sleep(3)

    print("extract_players_details task executed.")

def extract_players_shots(url, tournament_id, season_id):
    print(tournament_id)    
    filesystem_client = service_client.get_file_system_client(container_name)
    paths = filesystem_client.get_paths(path="/rawdata/events_lineups")

    file_paths = [path.name for path in paths]

    for file_path in file_paths:
        
        event_id = file_path.split("_")[-2]
        players_played_ids = []

        file_client = filesystem_client.get_file_client(file_path)
        download = file_client.download_file()
        content = download.readall().decode('utf-8')
        lineups_data = json.loads(content)
        
        for result_index in range(len(lineups_data['home']['players'])):
            if len(lineups_data['home']['players'][result_index].get("statistics","")) > 0:
                if lineups_data['home']['players'][result_index]['statistics']['minutesPlayed'] > 0:
                    players_played_ids.append(lineups_data['home']['players'][result_index]['player']['id'])
        for result_index in range(len(lineups_data['away']['players'])):
            if len(lineups_data['away']['players'][result_index].get("statistics","")) > 0:
                if lineups_data['away']['players'][result_index]['statistics']['minutesPlayed'] > 0:
                    players_played_ids.append(lineups_data['away']['players'][result_index]['player']['id'])

        for player_id in players_played_ids:
            with webdriver.Remote(
                command_executor=f'{remote_webdriver}:4444/wd/hub',
                options=options
            ) as driver:
                driver.get(url+f"{event_id}/shotmap/player/{player_id}")
                driver.implicitly_wait(10)
                pre_element = driver.find_element(By.TAG_NAME, 'pre')
                api_response = pre_element.text

                api_response = json.loads(api_response)

                if 'error' not in api_response:

                    file_client = filesystem_client.get_file_client(f"/rawdata/events_players_shots/{tournament_id}_{season_id}_{event_id}_{player_id}_player_shots.json")

                    file_client.upload_data(
                        json.dumps(api_response, indent=4, ensure_ascii=False),
                        overwrite=True
                    )

                if player_id != players_played_ids[-1]:
                    time.sleep(3)

    print("extract_players_shots task executed.")

def extract_teams(url, tournament_id, season_id):

    with webdriver.Remote(
        command_executor=f'{remote_webdriver}:4444/wd/hub',
        options=options
    ) as driver:
        driver.get(url+f"{tournament_id}"+"/season/"+f"{season_id}"+"/standings/total")
        driver.implicitly_wait(10)
        pre_element = driver.find_element(By.TAG_NAME, 'pre')
        api_response = pre_element.text

        api_response = json.loads(api_response)

        file_client = filesystem_client.get_file_client(f"/rawdata/teams/{tournament_id}_{season_id}_teams.json")

        file_client.upload_data(
            json.dumps(api_response, indent=4, ensure_ascii=False),
            overwrite=True
        )

    print("extract_teams task executed.")