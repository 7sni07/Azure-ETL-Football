from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.sofascore_extraction_pipeline import extract_tournament_data, extract_events_data, extract_events_ids, extract_events_lineups, extract_events_statistics, extract_events_best_players, extract_events_incidents, extract_players, extract_players_ids, extract_teams, extract_players_details,extract_players_shots

tournament_id = 937
season_id = 54108

dag = DAG(
    dag_id='sofascore_scraping',
    default_args={
        "owner": "Housni Achbouq",
        "start_date": datetime(2024, 11, 10),
        "retries": 3, 
    },
    schedule_interval=None,
    catchup=False
)



extract_tournament_data = PythonOperator(
    task_id="extract_tournament_data",
    python_callable=extract_tournament_data,
    provide_context=True,
    op_kwargs={ "url": f"https://api.sofascore.com/api/v1/unique-tournament/", "tournament_id": tournament_id, "season_id": season_id},
    dag=dag
)

extract_events_data = PythonOperator(
    task_id="extract_events_data",
    python_callable=extract_events_data,
    provide_context=True,
    op_kwargs={ "tournament_id": tournament_id ,"url": f"https://api.sofascore.com/api/v1/unique-tournament/937/season/{season_id}/events/round/"},
    dag=dag
)

extract_events_ids = PythonOperator(
    task_id="extract_events_ids",
    python_callable=extract_events_ids,
    provide_context=True,
    op_kwargs={"tournament_id": tournament_id},
    dag=dag
)

extract_events_lineups = PythonOperator(
    task_id="extract_events_lineups",
    python_callable=extract_events_lineups,
    provide_context=True,
    op_kwargs={"url": "https://www.sofascore.com/api/v1/event/", "tournament_id": tournament_id},
    dag=dag
)

extract_events_statistics = PythonOperator(
    task_id="extract_events_statistics",
    python_callable=extract_events_statistics,
    provide_context=True,
    op_kwargs={"url": "https://www.sofascore.com/api/v1/event/", "tournament_id": tournament_id},
    dag=dag
)

extract_events_best_players = PythonOperator(
    task_id="extract_events_best_players",
    python_callable=extract_events_best_players,
    provide_context=True,
    op_kwargs={"url": "https://www.sofascore.com/api/v1/event/", "tournament_id": tournament_id},
    dag=dag
)

extract_events_incidents = PythonOperator(
    task_id="extract_events_incidents",
    python_callable=extract_events_incidents,
    provide_context=True,
    op_kwargs={"url": "https://www.sofascore.com/api/v1/event/", "tournament_id": tournament_id},
    dag=dag
)

extract_players = PythonOperator(
    task_id="extract_players",
    python_callable=extract_players,
    provide_context=True,
    op_kwargs={"url": "https://www.sofascore.com/api/v1/unique-tournament/", "tournament_id": tournament_id, "season_id": season_id},
    dag=dag
)


extract_players_ids = PythonOperator(
    task_id="extract_players_ids",
    python_callable=extract_players_ids,
    provide_context=True,
    op_kwargs={"tournament_id": tournament_id, "season_id": season_id},
    dag=dag
)

extract_players_details = PythonOperator(
    task_id="extract_players_details",
    python_callable=extract_players_details,
    provide_context=True,
    op_kwargs={"url":"https://api.sofascore.com/api/v1/player/", "tournament_id": tournament_id, "season_id": season_id},
    dag=dag
)

extract_players_shots = PythonOperator(
    task_id="extract_players_shots",
    python_callable=extract_players_shots,
    provide_context=True,
    op_kwargs={"url":"https://www.sofascore.com/api/v1/event/", "tournament_id": tournament_id, "season_id": season_id},
    dag=dag
)

extract_teams = PythonOperator(
    task_id="extract_teams",
    python_callable=extract_teams,
    provide_context=True,
    op_kwargs={"url": "https://www.sofascore.com/api/v1/unique-tournament/", "tournament_id": tournament_id, "season_id": season_id},
    dag=dag
)

extract_tournament_data >> extract_events_data >> extract_events_ids >> extract_events_lineups >> extract_events_statistics >> extract_events_best_players >> extract_events_incidents >> extract_players >> extract_players_ids >> extract_players_details >> extract_players_shots >> extract_teams