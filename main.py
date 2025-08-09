#!/usr/bin/env python3
"""
Yahoo Fantasy Data Pipeline Main Program
Contains business logic orchestration and user interface
"""

import os
import sys
from typing import Dict, List

# Ensure proper module import
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from yahoo_api_data import YahooFantasyDataPipeline
from database.database_ops import DatabaseOps


def pipeline_process(pipeline: YahooFantasyDataPipeline) -> bool:
    """Business logic orchestration"""
    
    # Ensure league basic info exists in database
    if not pipeline.ensure_league_exists_in_db():
        print("❌ Cannot get league basic info, please select league first")
        return False

    if not pipeline.selected_league:
        return False
    
    # League settings
    league_key = pipeline.selected_league['league_key']
    settings_data = pipeline.fetch_api_league_settings(league_key)
    pipeline.db_writer.write_league_settings(league_key, settings_data)

    # Season schedule
    dates_data = pipeline.parse_season_dates()
    pipeline.db_writer.write_date_dimensions_batch(dates_data)
        
    # Player data
    players_data = pipeline.fetch_api_players(league_key)
    pipeline.db_writer.write_players_batch(players_data, league_key)
    
    # Team data
    teams_data = pipeline.fetch_api_teams(league_key)
    pipeline.db_writer.write_teams_to_db(teams_data, league_key)

    # Get all transactions
    all_transactions = pipeline.fetch_api_transactions(league_key)
    pipeline.db_writer.write_transactions_to_db(all_transactions, league_key)
        
    # League standings data
    standings_data = pipeline.fetch_api_league_standings(league_key)
    teams_standings = pipeline.parse_league_standings(standings_data)
    for team_info in teams_standings:
        season = pipeline.selected_league.get('season')
        pipeline.db_writer.write_league_standings_from_data(team_info, league_key, season)

    # League matchups data
    team_keys = pipeline.parse_team_keys(teams_data)
    for team_key in team_keys:
        matchups_data = pipeline.fetch_api_team_matchups(team_key)
        pipeline.db_writer.process_team_matchups_to_db(matchups_data, team_key, league_key, season)



    # Get player season stats
    player_season = pipeline.fetch_api_players_stats_season(players_data, league_key)
    pipeline.db_writer.process_player_season_stats_data(player_season, league_key, season)

    
    start_date, end_date = pipeline.get_time_range()

    print("🚀 Fetching daily roster")
    # Get roster data for specified time range
    roster_data = pipeline.fetch_team_rosters_time_range(teams_data, start_date, end_date)
    
    # Write roster data to database
    season = pipeline.selected_league.get('season')
    for roster_entry in roster_data:
        pipeline.db_writer.process_roster_data_to_db(
            roster_entry['roster_data'], 
            roster_entry['team_key'], 
            league_key, 
            season
        )

    print("🚀 Fetching player daily stats")
    players_stats = pipeline.fetch_api_players_stats_time_range(players_data, league_key, start_date, end_date)
    
    # Write player daily stats to database
    for stats_entry in players_stats:
        pipeline.db_writer._process_player_daily_stats_data(
            stats_entry['stats_data'], 
            league_key, 
            season, 
            stats_entry['date']
        )

    return True


def print_league_selection_info(leagues_data) -> List[Dict]:
    """Print league selection information"""
    print("\n" + "="*80)
    print("Available Fantasy Leagues")
    print("="*80)
    
    all_leagues = []
    league_counter = 1
    
    for leagues in leagues_data.values():
        for league in leagues:
            league_info = {
                'index': league_counter,
                'league_key': league.get('league_key'),
                'name': league.get('name', 'Unknown League'),
                'season': league.get('season', 'Unknown Season'),
                'num_teams': league.get('num_teams', 0),
                'game_code': league.get('game_code', 'Unknown Sport'),
                'scoring_type': league.get('scoring_type', 'Unknown'),
                'is_finished': league.get('is_finished', 0) == 1
            }
            all_leagues.append(league_info)
            
            # Print league info
            status = "Finished" if league_info['is_finished'] else "Ongoing"
            print(f"{league_counter:2d}. {league_info['name']}")
            print(f"    League ID: {league_info['league_key']}")
            print(f"    Sport: {league_info['game_code'].upper()} | Season: {league_info['season']} | Status: {status}")
            print(f"    Teams: {league_info['num_teams']} | Scoring: {league_info['scoring_type']}")
            print()
            
            league_counter += 1
    
    print("="*80)
    return all_leagues


def select_league(pipeline: YahooFantasyDataPipeline) -> bool:
    """Get basic data and select league"""
    print("🚀 Getting league data...")
    
    games_data = pipeline.fetch_api_games()
    pipeline.db_writer.write_games_data(games_data)
    
    # Extract game keys and get league data
    game_keys = pipeline.parse_game_keys(games_data)
    
    all_leagues = {}

    leagues_data = pipeline.extract_leagues_from_db()

    if not leagues_data:
        for i, game_key in enumerate(game_keys):
            leagues_data = pipeline.fetch_api_leagues(game_key)
            if leagues_data:
                extracted_leagues = pipeline.parse_leagues(leagues_data, game_key)
                if extracted_leagues:
                    all_leagues[game_key] = extracted_leagues
            
            if i < len(game_keys) - 1:
                pipeline.wait()
        
        if all_leagues:
            # Write league data to database
            pipeline.db_writer.write_leagues_data(all_leagues)
            return all_leagues

    if not leagues_data:
        print("✗ Unable to get league data")
        return False
    
    # Select league
    selected_league = None

    # League selection
    all_leagues = print_league_selection_info(leagues_data)
    
    while True:
        try:
            choice = input(f"Please select league (1-{len(all_leagues)}): ").strip()
            
            if not choice:
                continue
                
            choice_num = int(choice)
            
            if 1 <= choice_num <= len(all_leagues):
                selected_league = all_leagues[choice_num - 1]
                print(f"\n✓ Selected league: {selected_league['name']} ({selected_league['league_key']})")
                pipeline.selected_league = selected_league
                return selected_league
            else:
                print(f"Please enter number between 1 and {len(all_leagues)}")
                
        except ValueError:
            print("Please enter valid number")
        except KeyboardInterrupt:
            print("\nUser cancelled selection")
            return None 


def run_interactive_menu(pipeline: YahooFantasyDataPipeline):
    """Interactive menu"""
    while True:
        print("\n=== Yahoo NBA Fantasy Data Tool ===")
        if pipeline.selected_league:
            print(f"Current league: {pipeline.selected_league['name']} ({pipeline.selected_league['league_key']})")
        else:
            print("Current league: Not selected")
        
        print("\n1. Select League")
        print("2. Fetch Data")
        print("3. Database Summary")
        print("4. Clear Database")
        print("0. Quit")

        choice = input("\nPlease select operation: ").strip()
        
        if choice == "0":
            print("Exit")
            break

        elif choice == "1":
            select_league(pipeline)

        elif choice == "2":
            if pipeline.ensure_league_selected():
                pipeline_process(pipeline)

        elif choice == "3":
            DatabaseOps.show_database_summary()

        elif choice == "4":
            DatabaseOps.clear_database(1)

        else:
            print("Invalid selection, please try again")


def main():
    """Main function - Yahoo NBA Fantasy data tool"""
    # Create data pipeline
    fetcher = YahooFantasyDataPipeline(delay=2, batch_size=100)
    
    try:
        # Run interactive menu
        run_interactive_menu(fetcher)
    
    finally:
        # Ensure resource cleanup
        fetcher.close()


if __name__ == "__main__":
    main()